package task

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flyteidl/clients/go/events"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	pluginCatalogMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog/mocks"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCoreMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	ioMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	pluginK8s "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	pluginK8sMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s/mocks"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	flyteMocks "github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1/mocks"
	"github.com/lyft/flytepropeller/pkg/controller/executors/mocks"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	nodeMocks "github.com/lyft/flytepropeller/pkg/controller/nodes/handler/mocks"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/codex"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/config"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/fakeplugins"
)

func Test_task_setDefault(t *testing.T) {
	type fields struct {
		defaultPlugin pluginCore.Plugin
	}
	type args struct {
		p pluginCore.Plugin
	}

	other := &pluginCoreMocks.Plugin{}
	other.On("GetID").Return("other")

	def := &pluginCoreMocks.Plugin{}
	def.On("GetID").Return("default")

	tests := []struct {
		name           string
		fields         fields
		args           args
		wantErr        bool
		defaultChanged bool
	}{
		{"no-default", fields{nil}, args{p: other}, false, true},
		{"default-exists", fields{defaultPlugin: def}, args{p: other}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tk := &Handler{
				defaultPlugin: tt.fields.defaultPlugin,
			}
			if err := tk.setDefault(context.TODO(), tt.args.p); (err != nil) != tt.wantErr {
				t.Errorf("Handler.setDefault() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.defaultChanged {
				assert.Equal(t, tk.defaultPlugin, tt.args.p)
			} else {
				assert.NotEqual(t, tk.defaultPlugin, tt.args.p)
			}
		})
	}
}

type testPluginRegistry struct {
	core []pluginCore.PluginEntry
	k8s  []pluginK8s.PluginEntry
}

func (t testPluginRegistry) GetCorePlugins() []pluginCore.PluginEntry {
	return t.core
}

func (t testPluginRegistry) GetK8sPlugins() []pluginK8s.PluginEntry {
	return t.k8s
}

func Test_task_Setup(t *testing.T) {
	corePluginType := "core"
	corePlugin := &pluginCoreMocks.Plugin{}
	corePlugin.On("GetID").Return(corePluginType)

	corePluginDefaultType := "coredefault"
	corePluginDefault := &pluginCoreMocks.Plugin{}
	corePluginDefault.On("GetID").Return(corePluginDefaultType)

	k8sPluginType := "k8s"
	k8sPlugin := &pluginK8sMocks.Plugin{}

	k8sPluginDefaultType := "k8sdefault"
	k8sPluginDefault := &pluginK8sMocks.Plugin{}

	corePluginEntry := pluginCore.PluginEntry{
		ID:                  corePluginType,
		RegisteredTaskTypes: []pluginCore.TaskType{corePluginType},
		LoadPlugin: func(ctx context.Context, iCtx pluginCore.SetupContext) (pluginCore.Plugin, error) {
			return corePlugin, nil
		},
	}
	corePluginEntryDefault := pluginCore.PluginEntry{
		IsDefault:           true,
		ID:                  corePluginDefaultType,
		RegisteredTaskTypes: []pluginCore.TaskType{corePluginDefaultType},
		LoadPlugin: func(ctx context.Context, iCtx pluginCore.SetupContext) (pluginCore.Plugin, error) {
			return corePluginDefault, nil
		},
	}
	k8sPluginEntry := pluginK8s.PluginEntry{
		ID:                  k8sPluginType,
		Plugin:              k8sPlugin,
		RegisteredTaskTypes: []pluginCore.TaskType{k8sPluginType},
		ResourceToWatch:     &v1.Pod{},
	}
	k8sPluginEntryDefault := pluginK8s.PluginEntry{
		IsDefault:           true,
		ID:                  k8sPluginDefaultType,
		Plugin:              k8sPluginDefault,
		RegisteredTaskTypes: []pluginCore.TaskType{k8sPluginDefaultType},
		ResourceToWatch:     &v1.Pod{},
	}

	type wantFields struct {
		pluginIDs       map[pluginCore.TaskType]string
		defaultPluginID string
	}
	tests := []struct {
		name     string
		registry PluginRegistryIface
		fields   wantFields
		wantErr  bool
	}{
		{"no-plugins", testPluginRegistry{}, wantFields{}, false},
		{"no-default-only-core", testPluginRegistry{
			core: []pluginCore.PluginEntry{corePluginEntry}, k8s: []pluginK8s.PluginEntry{},
		}, wantFields{
			pluginIDs: map[pluginCore.TaskType]string{corePluginType: corePluginType},
		}, false},
		{"no-default-only-k8s", testPluginRegistry{
			core: []pluginCore.PluginEntry{}, k8s: []pluginK8s.PluginEntry{k8sPluginEntry},
		}, wantFields{
			pluginIDs: map[pluginCore.TaskType]string{k8sPluginType: k8sPluginType},
		}, false},
		{"no-default", testPluginRegistry{
			core: []pluginCore.PluginEntry{corePluginEntry}, k8s: []pluginK8s.PluginEntry{k8sPluginEntry},
		}, wantFields{
			pluginIDs: map[pluginCore.TaskType]string{corePluginType: corePluginType, k8sPluginType: k8sPluginType},
		}, false},
		{"only-default-core", testPluginRegistry{
			core: []pluginCore.PluginEntry{corePluginEntry, corePluginEntryDefault}, k8s: []pluginK8s.PluginEntry{k8sPluginEntry},
		}, wantFields{
			pluginIDs:       map[pluginCore.TaskType]string{corePluginType: corePluginType, corePluginDefaultType: corePluginDefaultType, k8sPluginType: k8sPluginType},
			defaultPluginID: corePluginDefaultType,
		}, false},
		{"only-default-k8s", testPluginRegistry{
			core: []pluginCore.PluginEntry{corePluginEntry}, k8s: []pluginK8s.PluginEntry{k8sPluginEntryDefault},
		}, wantFields{
			pluginIDs:       map[pluginCore.TaskType]string{corePluginType: corePluginType, k8sPluginDefaultType: k8sPluginDefaultType},
			defaultPluginID: k8sPluginDefaultType,
		}, false},
		{"default-both", testPluginRegistry{
			core: []pluginCore.PluginEntry{corePluginEntry, corePluginEntryDefault}, k8s: []pluginK8s.PluginEntry{k8sPluginEntry, k8sPluginEntryDefault},
		}, wantFields{
			pluginIDs:       map[pluginCore.TaskType]string{corePluginType: corePluginType, corePluginDefaultType: corePluginDefaultType, k8sPluginType: k8sPluginType, k8sPluginDefaultType: k8sPluginDefaultType},
			defaultPluginID: corePluginDefaultType,
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sCtx := &nodeMocks.SetupContext{}
			fakeKubeClient := mocks.NewFakeKubeClient()
			sCtx.On("KubeClient").Return(fakeKubeClient)
			sCtx.On("OwnerKind").Return("test")
			sCtx.On("EnqueueOwner").Return(pluginCore.EnqueueOwner(func(name types.NamespacedName) error { return nil }))
			sCtx.On("MetricsScope").Return(promutils.NewTestScope())

			tk, err := New(context.TODO(), mocks.NewFakeKubeClient(), &pluginCatalogMocks.Client{}, promutils.NewTestScope())
			assert.NoError(t, err)
			tk.pluginRegistry = tt.registry
			if err := tk.Setup(context.TODO(), sCtx); err != nil {
				if !tt.wantErr {
					t.Errorf("Handler.Setup() error not expected. got = %v", err)
				}
			} else {
				if tt.wantErr {
					t.Errorf("Handler.Setup() error expected, got none!")
				}
				for k, v := range tt.fields.pluginIDs {
					p, ok := tk.plugins[k]
					if assert.True(t, ok, "plugin %s not found", k) {
						assert.Equal(t, v, p.GetID())
					}
				}
				if tt.fields.defaultPluginID != "" {
					if assert.NotNil(t, tk.defaultPlugin, "default plugin is nil") {
						assert.Equal(t, tk.defaultPlugin.GetID(), tt.fields.defaultPluginID)
					}
				}
			}
		})
	}
}

func Test_task_ResolvePlugin(t *testing.T) {
	defaultID := "default"
	someID := "some"
	defaultPlugin := &pluginCoreMocks.Plugin{}
	defaultPlugin.On("GetID").Return(defaultID)
	somePlugin := &pluginCoreMocks.Plugin{}
	somePlugin.On("GetID").Return(someID)
	type fields struct {
		plugins       map[pluginCore.TaskType]pluginCore.Plugin
		defaultPlugin pluginCore.Plugin
	}
	type args struct {
		ttype string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"no-plugins", fields{}, args{}, "", true},
		{"default",
			fields{
				defaultPlugin: defaultPlugin,
			}, args{ttype: someID}, defaultID, false},
		{"actual",
			fields{
				plugins: map[pluginCore.TaskType]pluginCore.Plugin{
					someID: somePlugin,
				},
				defaultPlugin: defaultPlugin,
			}, args{ttype: someID}, someID, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tk := Handler{
				plugins:       tt.fields.plugins,
				defaultPlugin: tt.fields.defaultPlugin,
			}
			got, err := tk.ResolvePlugin(context.TODO(), tt.args.ttype)
			if (err != nil) != tt.wantErr {
				t.Errorf("Handler.ResolvePlugin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				assert.Equal(t, tt.want, got.GetID())
			}
		})
	}
}

type fakeBufferedTaskEventRecorder struct {
	evs []*event.TaskExecutionEvent
}

func (f *fakeBufferedTaskEventRecorder) RecordTaskEvent(ctx context.Context, ev *event.TaskExecutionEvent) error {
	f.evs = append(f.evs, ev)
	return nil
}

type taskNodeStateHolder struct {
	s handler.TaskNodeState
}

func (t *taskNodeStateHolder) PutTaskNodeState(s handler.TaskNodeState) error {
	t.s = s
	return nil
}

func (t taskNodeStateHolder) PutBranchNode(s handler.BranchNodeState) error {
	panic("not implemented")
}

func (t taskNodeStateHolder) PutWorkflowNodeState(s handler.WorkflowNodeState) error {
	panic("not implemented")
}

func (t taskNodeStateHolder) PutDynamicNodeState(s handler.DynamicNodeState) error {
	panic("not implemented")
}

func Test_task_Handle_NoCatalog(t *testing.T) {

	createNodeContext := func(pluginPhase pluginCore.Phase, pluginVer uint32, pluginResp fakeplugins.NextPhaseState, recorder events.TaskEventRecorder, ttype string, s *taskNodeStateHolder) *nodeMocks.NodeExecutionContext {
		wfExecID := &core.WorkflowExecutionIdentifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
		}

		nm := &nodeMocks.NodeExecutionMetadata{}
		nm.On("GetAnnotations").Return(map[string]string{})
		nm.On("GetExecutionID").Return(v1alpha1.WorkflowExecutionIdentifier{
			WorkflowExecutionIdentifier: wfExecID,
		})
		nm.On("GetK8sServiceAccount").Return("service-account")
		nm.On("GetLabels").Return(map[string]string{})
		nm.On("GetNamespace").Return("namespace")
		nm.On("GetOwnerID").Return(types.NamespacedName{Namespace: "namespace", Name: "name"})
		nm.On("GetOwnerReference").Return(v12.OwnerReference{
			Kind: "sample",
			Name: "name",
		})

		tk := &core.TaskTemplate{
			Id:   nil,
			Type: "test",
			Metadata: &core.TaskMetadata{
				Discoverable: false,
			},
			Interface: &core.TypedInterface{
				Outputs: &core.VariableMap{
					Variables: map[string]*core.Variable{
						"x": {
							Type: &core.LiteralType{
								Type: &core.LiteralType_Simple{
									Simple: core.SimpleType_BOOLEAN,
								},
							},
						},
					},
				},
			},
		}
		taskID := &core.Identifier{}
		tr := &nodeMocks.TaskReader{}
		tr.On("GetTaskID").Return(taskID)
		tr.On("GetTaskType").Return(ttype)
		tr.On("Read", mock.Anything).Return(tk, nil)

		ns := &flyteMocks.ExecutableNodeStatus{}
		ns.On("GetDataDir").Return(storage.DataReference("data-dir"))

		res := &v1.ResourceRequirements{}
		n := &flyteMocks.ExecutableNode{}
		n.On("GetResources").Return(res)

		ir := &ioMocks.InputReader{}
		ir.On("GetInputPath").Return(storage.DataReference("input"))
		nCtx := &nodeMocks.NodeExecutionContext{}
		nCtx.On("NodeExecutionMetadata").Return(nm)
		nCtx.On("Node").Return(n)
		nCtx.On("InputReader").Return(ir)
		nCtx.On("DataStore").Return(storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope()))
		nCtx.On("CurrentAttempt").Return(uint32(1))
		nCtx.On("TaskReader").Return(tr)
		nCtx.On("MaxDatasetSizeBytes").Return(int64(1))
		nCtx.On("NodeStatus").Return(ns)
		nCtx.On("NodeID").Return("n1")
		nCtx.On("EventsRecorder").Return(recorder)
		nCtx.On("EnqueueOwner").Return(nil)

		st := bytes.NewBuffer([]byte{})
		cod := codex.GobStateCodec{}
		assert.NoError(t, cod.Encode(pluginResp, st))
		nr := &nodeMocks.NodeStateReader{}
		nr.On("GetTaskNodeState").Return(handler.TaskNodeState{
			PluginState:        st.Bytes(),
			PluginPhase:        pluginPhase,
			PluginPhaseVersion: pluginVer,
		})
		nCtx.On("NodeStateReader").Return(nr)
		nCtx.On("NodeStateWriter").Return(s)
		return nCtx
	}

	type args struct {
		startingPluginPhase        pluginCore.Phase
		startingPluginPhaseVersion int
		expectedState              fakeplugins.NextPhaseState
	}
	type want struct {
		handlerPhase    handler.EPhase
		wantErr         bool
		event           bool
		eventPhase      core.TaskExecution_Phase
		skipStateUpdate bool
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			"success",
			args{
				startingPluginPhase:        pluginCore.PhaseUndefined,
				startingPluginPhaseVersion: 0,
				expectedState: fakeplugins.NextPhaseState{
					Phase:        pluginCore.PhaseSuccess,
					PhaseVersion: 0,
					TaskInfo:     nil,
					TaskErr:      nil,
					OutputExists: true,
					OrError:      false,
				},
			},
			want{
				handlerPhase: handler.EPhaseSuccess,
				event:        true,
				eventPhase:   core.TaskExecution_SUCCEEDED,
			},
		},
		{
			"success-output-missing",
			args{
				startingPluginPhase:        pluginCore.PhaseUndefined,
				startingPluginPhaseVersion: 0,
				expectedState: fakeplugins.NextPhaseState{
					Phase:        pluginCore.PhaseSuccess,
					PhaseVersion: 0,
					OutputExists: false,
				},
			},
			want{
				handlerPhase: handler.EPhaseRetryableFailure,
				event:        true,
				eventPhase:   core.TaskExecution_FAILED,
			},
		},
		{
			"success-output-err-recoverable",
			args{
				startingPluginPhase:        pluginCore.PhaseUndefined,
				startingPluginPhaseVersion: 0,
				expectedState: fakeplugins.NextPhaseState{
					Phase:        pluginCore.PhaseSuccess,
					PhaseVersion: 0,
					TaskInfo:     nil,
					TaskErr: &io.ExecutionError{
						IsRecoverable: true,
					},
					OutputExists: false,
					OrError:      false,
				},
			},
			want{
				handlerPhase: handler.EPhaseRetryableFailure,
				event:        true,
				eventPhase:   core.TaskExecution_FAILED,
			},
		},
		{
			"success-output-err-non-recoverable",
			args{
				startingPluginPhase:        pluginCore.PhaseUndefined,
				startingPluginPhaseVersion: 0,
				expectedState: fakeplugins.NextPhaseState{
					Phase:        pluginCore.PhaseSuccess,
					PhaseVersion: 0,
					TaskInfo:     nil,
					TaskErr: &io.ExecutionError{
						IsRecoverable: false,
					},
					OutputExists: false,
					OrError:      false,
				},
			},
			want{
				handlerPhase: handler.EPhaseFailed,
				event:        true,
				eventPhase:   core.TaskExecution_FAILED,
			},
		},
		{
			"running",
			args{
				startingPluginPhase:        pluginCore.PhaseUndefined,
				startingPluginPhaseVersion: 0,
				expectedState: fakeplugins.NextPhaseState{
					Phase:        pluginCore.PhaseRunning,
					PhaseVersion: 0,
					TaskInfo: &pluginCore.TaskInfo{
						Logs: []*core.TaskLog{
							{Name: "x", Uri: "y"},
						},
					},
				},
			},
			want{
				handlerPhase: handler.EPhaseRunning,
				event:        true,
				eventPhase:   core.TaskExecution_RUNNING,
			},
		},
		{
			"running-no-event-phaseversion",
			args{
				startingPluginPhase:        pluginCore.PhaseRunning,
				startingPluginPhaseVersion: 1,
				expectedState: fakeplugins.NextPhaseState{
					Phase:        pluginCore.PhaseRunning,
					PhaseVersion: 1,
					TaskInfo: &pluginCore.TaskInfo{
						Logs: []*core.TaskLog{
							{Name: "x", Uri: "y"},
						},
					},
				},
			},
			want{
				handlerPhase:    handler.EPhaseRunning,
				event:           false,
				skipStateUpdate: true,
			},
		},
		{
			"running-error",
			args{
				startingPluginPhase:        pluginCore.PhaseRunning,
				startingPluginPhaseVersion: 1,
				expectedState: fakeplugins.NextPhaseState{
					OrError: true,
				},
			},
			want{
				handlerPhase: handler.EPhaseUndefined,
				event:        false,
				wantErr:      true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &taskNodeStateHolder{}
			ev := &fakeBufferedTaskEventRecorder{}
			nCtx := createNodeContext(tt.args.startingPluginPhase, uint32(tt.args.startingPluginPhaseVersion), tt.args.expectedState, ev, "test", state)
			c := &pluginCatalogMocks.Client{}
			tk := Handler{
				plugins: map[pluginCore.TaskType]pluginCore.Plugin{
					"test": fakeplugins.NewPhaseBasedPlugin(),
				},
				catalog: c,
				barrierCache: newLRUBarrier(context.TODO(), config.BarrierConfig{
					Enabled: false,
				}),
			}
			got, err := tk.Handle(context.TODO(), nCtx)
			if (err != nil) != tt.want.wantErr {
				t.Errorf("Handler.Handle() error = %v, wantErr %v", err, tt.want.wantErr)
				return
			}
			if err == nil {
				assert.Equal(t, tt.want.handlerPhase.String(), got.Info().GetPhase().String())
				if tt.want.event {
					if assert.Equal(t, 1, len(ev.evs)) {
						e := ev.evs[0]
						assert.Equal(t, tt.want.eventPhase.String(), e.Phase.String())
						if tt.args.expectedState.TaskInfo != nil {
							assert.Equal(t, tt.args.expectedState.TaskInfo.Logs, e.Logs)
						}
					}
				} else {
					assert.Equal(t, 0, len(ev.evs))
				}
				expectedPhase := tt.args.expectedState.Phase
				if tt.args.expectedState.Phase.IsSuccess() && !tt.args.expectedState.OutputExists {
					expectedPhase = pluginCore.PhaseRetryableFailure
				}
				if tt.args.expectedState.TaskErr != nil {
					if tt.args.expectedState.TaskErr.IsRecoverable {
						expectedPhase = pluginCore.PhaseRetryableFailure
					} else {
						expectedPhase = pluginCore.PhasePermanentFailure
					}
				}
				if tt.want.skipStateUpdate {
					assert.Equal(t, pluginCore.PhaseUndefined, state.s.PluginPhase)
					assert.Equal(t, uint32(0), state.s.PluginPhaseVersion)
				} else {
					assert.Equal(t, expectedPhase.String(), state.s.PluginPhase.String())
					assert.Equal(t, tt.args.expectedState.PhaseVersion, state.s.PluginPhaseVersion)
				}
			}
		})
	}
}

func Test_task_Handle_Catalog(t *testing.T) {

	createNodeContext := func(recorder events.TaskEventRecorder, ttype string, s *taskNodeStateHolder) *nodeMocks.NodeExecutionContext {
		wfExecID := &core.WorkflowExecutionIdentifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
		}

		nm := &nodeMocks.NodeExecutionMetadata{}
		nm.On("GetAnnotations").Return(map[string]string{})
		nm.On("GetExecutionID").Return(v1alpha1.WorkflowExecutionIdentifier{
			WorkflowExecutionIdentifier: wfExecID,
		})
		nm.On("GetK8sServiceAccount").Return("service-account")
		nm.On("GetLabels").Return(map[string]string{})
		nm.On("GetNamespace").Return("namespace")
		nm.On("GetOwnerID").Return(types.NamespacedName{Namespace: "namespace", Name: "name"})
		nm.On("GetOwnerReference").Return(v12.OwnerReference{
			Kind: "sample",
			Name: "name",
		})

		taskID := &core.Identifier{}
		tk := &core.TaskTemplate{
			Id:   taskID,
			Type: "test",
			Metadata: &core.TaskMetadata{
				Discoverable: true,
			},
			Interface: &core.TypedInterface{
				Outputs: &core.VariableMap{
					Variables: map[string]*core.Variable{
						"x": {
							Type: &core.LiteralType{
								Type: &core.LiteralType_Simple{
									Simple: core.SimpleType_BOOLEAN,
								},
							},
						},
					},
				},
			},
		}
		tr := &nodeMocks.TaskReader{}
		tr.On("GetTaskID").Return(taskID)
		tr.On("GetTaskType").Return(ttype)
		tr.On("Read", mock.Anything).Return(tk, nil)

		ns := &flyteMocks.ExecutableNodeStatus{}
		ns.On("GetDataDir").Return(storage.DataReference("data-dir"))

		res := &v1.ResourceRequirements{}
		n := &flyteMocks.ExecutableNode{}
		n.On("GetResources").Return(res)

		ir := &ioMocks.InputReader{}
		ir.On("GetInputPath").Return(storage.DataReference("input"))
		nCtx := &nodeMocks.NodeExecutionContext{}
		nCtx.On("NodeExecutionMetadata").Return(nm)
		nCtx.On("Node").Return(n)
		nCtx.On("InputReader").Return(ir)
		nCtx.On("DataStore").Return(storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope()))
		nCtx.On("CurrentAttempt").Return(uint32(1))
		nCtx.On("TaskReader").Return(tr)
		nCtx.On("MaxDatasetSizeBytes").Return(int64(1))
		nCtx.On("NodeStatus").Return(ns)
		nCtx.On("NodeID").Return("n1")
		nCtx.On("EventsRecorder").Return(recorder)
		nCtx.On("EnqueueOwner").Return(nil)

		st := bytes.NewBuffer([]byte{})
		cod := codex.GobStateCodec{}
		assert.NoError(t, cod.Encode(&fakeplugins.NextPhaseState{
			Phase:        pluginCore.PhaseSuccess,
			OutputExists: true,
		}, st))
		nr := &nodeMocks.NodeStateReader{}
		nr.On("GetTaskNodeState").Return(handler.TaskNodeState{
			PluginState: st.Bytes(),
		})
		nCtx.On("NodeStateReader").Return(nr)
		nCtx.On("NodeStateWriter").Return(s)
		return nCtx
	}

	type args struct {
		catalogFetch      bool
		catalogFetchError bool
		catalogWriteError bool
	}
	type want struct {
		handlerPhase handler.EPhase
		wantErr      bool
		eventPhase   core.TaskExecution_Phase
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			"cache-hit",
			args{
				catalogFetch:      true,
				catalogWriteError: true,
			},
			want{
				handlerPhase: handler.EPhaseSuccess,
				eventPhase:   core.TaskExecution_SUCCEEDED,
			},
		},
		{
			"cache-err",
			args{
				catalogFetchError: true,
				catalogWriteError: true,
			},
			want{
				handlerPhase: handler.EPhaseSuccess,
				eventPhase:   core.TaskExecution_SUCCEEDED,
			},
		},
		{
			"cache-write",
			args{},
			want{
				handlerPhase: handler.EPhaseSuccess,
				eventPhase:   core.TaskExecution_SUCCEEDED,
			},
		},
		{
			"cache-write-err",
			args{
				catalogWriteError: true,
			},
			want{
				handlerPhase: handler.EPhaseSuccess,
				eventPhase:   core.TaskExecution_SUCCEEDED,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &taskNodeStateHolder{}
			ev := &fakeBufferedTaskEventRecorder{}
			nCtx := createNodeContext(ev, "test", state)
			c := &pluginCatalogMocks.Client{}
			if tt.args.catalogFetch {
				or := &ioMocks.OutputReader{}
				or.On("Read", mock.Anything).Return(&core.LiteralMap{}, nil, nil)
				c.On("Get", mock.Anything, mock.Anything).Return(or, nil)
			} else {
				c.On("Get", mock.Anything, mock.Anything).Return(nil, nil)
			}
			if tt.args.catalogFetchError {
				c.On("Get", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("failed to read from catalog"))
			}
			if tt.args.catalogWriteError {
				c.On("Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("failed to write to catalog"))
			} else {
				c.On("Put", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			}
			tk, err := New(context.TODO(), mocks.NewFakeKubeClient(), c, promutils.NewTestScope())
			assert.NoError(t, err)
			tk.plugins = map[pluginCore.TaskType]pluginCore.Plugin{
				"test": fakeplugins.NewPhaseBasedPlugin(),
			}
			tk.catalog = c
			got, err := tk.Handle(context.TODO(), nCtx)
			if (err != nil) != tt.want.wantErr {
				t.Errorf("Handler.Handle() error = %v, wantErr %v", err, tt.want.wantErr)
				return
			}
			if err == nil {
				assert.Equal(t, tt.want.handlerPhase.String(), got.Info().GetPhase().String())
				if assert.Equal(t, 1, len(ev.evs)) {
					e := ev.evs[0]
					assert.Equal(t, tt.want.eventPhase.String(), e.Phase.String())
				}
				assert.Equal(t, pluginCore.PhaseSuccess.String(), state.s.PluginPhase.String())
				assert.Equal(t, uint32(0), state.s.PluginPhaseVersion)
				if tt.args.catalogFetch {
					if assert.NotNil(t, got.Info().GetInfo().TaskNodeInfo) {
						assert.True(t, got.Info().GetInfo().TaskNodeInfo.CacheHit)
					}
					assert.NotNil(t, got.Info().GetInfo().OutputInfo)
					s := storage.DataReference("/data-dir/outputs.pb")
					assert.Equal(t, s, got.Info().GetInfo().OutputInfo.OutputURI)
					r, err := nCtx.DataStore().Head(context.TODO(), s)
					assert.NoError(t, err)
					assert.True(t, r.Exists())
				}
			}
		})
	}
}

func Test_task_Handle_Barrier(t *testing.T) {

	createNodeContext := func(recorder events.TaskEventRecorder, ttype string, s *taskNodeStateHolder, prevBarrierClockTick uint32) *nodeMocks.NodeExecutionContext {
		wfExecID := &core.WorkflowExecutionIdentifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
		}

		nm := &nodeMocks.NodeExecutionMetadata{}
		nm.On("GetAnnotations").Return(map[string]string{})
		nm.On("GetExecutionID").Return(v1alpha1.WorkflowExecutionIdentifier{
			WorkflowExecutionIdentifier: wfExecID,
		})
		nm.On("GetK8sServiceAccount").Return("service-account")
		nm.On("GetLabels").Return(map[string]string{})
		nm.On("GetNamespace").Return("namespace")
		nm.On("GetOwnerID").Return(types.NamespacedName{Namespace: "namespace", Name: "name"})
		nm.On("GetOwnerReference").Return(v12.OwnerReference{
			Kind: "sample",
			Name: "name",
		})

		taskID := &core.Identifier{}
		tk := &core.TaskTemplate{
			Id:   taskID,
			Type: "test",
			Metadata: &core.TaskMetadata{
				Discoverable: false,
			},
			Interface: &core.TypedInterface{
				Outputs: &core.VariableMap{
					Variables: map[string]*core.Variable{
						"x": {
							Type: &core.LiteralType{
								Type: &core.LiteralType_Simple{
									Simple: core.SimpleType_BOOLEAN,
								},
							},
						},
					},
				},
			},
		}
		tr := &nodeMocks.TaskReader{}
		tr.On("GetTaskID").Return(taskID)
		tr.On("GetTaskType").Return(ttype)
		tr.On("Read", mock.Anything).Return(tk, nil)

		ns := &flyteMocks.ExecutableNodeStatus{}
		ns.On("GetDataDir").Return(storage.DataReference("data-dir"))

		res := &v1.ResourceRequirements{}
		n := &flyteMocks.ExecutableNode{}
		n.On("GetResources").Return(res)

		ir := &ioMocks.InputReader{}
		ir.On("GetInputPath").Return(storage.DataReference("input"))
		nCtx := &nodeMocks.NodeExecutionContext{}
		nCtx.On("NodeExecutionMetadata").Return(nm)
		nCtx.On("Node").Return(n)
		nCtx.On("InputReader").Return(ir)
		nCtx.On("DataStore").Return(storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope()))
		nCtx.On("CurrentAttempt").Return(uint32(1))
		nCtx.On("TaskReader").Return(tr)
		nCtx.On("MaxDatasetSizeBytes").Return(int64(1))
		nCtx.On("NodeStatus").Return(ns)
		nCtx.On("NodeID").Return("n1")
		nCtx.On("EventsRecorder").Return(recorder)
		nCtx.On("EnqueueOwner").Return(nil)

		st := bytes.NewBuffer([]byte{})
		cod := codex.GobStateCodec{}
		assert.NoError(t, cod.Encode(&fakeplugins.NextPhaseState{
			Phase:        pluginCore.PhaseSuccess,
			OutputExists: true,
		}, st))
		nr := &nodeMocks.NodeStateReader{}
		nr.On("GetTaskNodeState").Return(handler.TaskNodeState{
			PluginState:      st.Bytes(),
			BarrierClockTick: prevBarrierClockTick,
		})
		nCtx.On("NodeStateReader").Return(nr)
		nCtx.On("NodeStateWriter").Return(s)
		return nCtx
	}

	trns := pluginCore.DoTransitionType(pluginCore.TransitionTypeBarrier, pluginCore.PhaseInfoQueued(time.Now(), 1, "z"))
	type args struct {
		prevTick  uint32
		btrnsTick uint32
		bTrns     *pluginCore.Transition
		res       []fakeplugins.HandleResponse
	}
	type wantBarrier struct {
		hit  bool
		tick uint32
	}
	type want struct {
		wantBarrer   wantBarrier
		handlerPhase handler.EPhase
		wantErr      bool
		eventPhase   core.TaskExecution_Phase
		pluginPhase  pluginCore.Phase
		pluginVer    uint32
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			"ephemeral-trns",
			args{
				res: []fakeplugins.HandleResponse{
					{T: pluginCore.DoTransitionType(pluginCore.TransitionTypeEphemeral, pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{}))},
				},
			},
			want{
				handlerPhase: handler.EPhaseRunning,
				eventPhase:   core.TaskExecution_RUNNING,
				pluginPhase:  pluginCore.PhaseRunning,
				pluginVer:    1,
			},
		},
		{
			"first-barrier-trns",
			args{
				res: []fakeplugins.HandleResponse{
					{T: pluginCore.DoTransitionType(pluginCore.TransitionTypeBarrier, pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{}))},
				},
			},
			want{
				wantBarrer: wantBarrier{
					hit:  true,
					tick: 1,
				},
				handlerPhase: handler.EPhaseRunning,
				eventPhase:   core.TaskExecution_RUNNING,
				pluginPhase:  pluginCore.PhaseRunning,
				pluginVer:    1,
			},
		},
		{
			"barrier-trns-replay",
			args{
				prevTick:  0,
				btrnsTick: 1,
				bTrns:     &trns,
			},
			want{
				wantBarrer: wantBarrier{
					hit:  true,
					tick: 1,
				},
				handlerPhase: handler.EPhaseRunning,
				eventPhase:   core.TaskExecution_QUEUED,
				pluginPhase:  pluginCore.PhaseQueued,
				pluginVer:    1,
			},
		},
		{
			"barrier-trns-next",
			args{
				prevTick:  1,
				btrnsTick: 1,
				bTrns:     &trns,
				res: []fakeplugins.HandleResponse{
					{T: pluginCore.DoTransitionType(pluginCore.TransitionTypeBarrier, pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{}))},
				},
			},
			want{
				wantBarrer: wantBarrier{
					hit:  true,
					tick: 2,
				},
				handlerPhase: handler.EPhaseRunning,
				eventPhase:   core.TaskExecution_RUNNING,
				pluginPhase:  pluginCore.PhaseRunning,
				pluginVer:    1,
			},
		},
		{
			"barrier-trns-restart-case",
			args{
				prevTick: 2,
				res: []fakeplugins.HandleResponse{
					{T: pluginCore.DoTransitionType(pluginCore.TransitionTypeBarrier, pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{}))},
				},
			},
			want{
				wantBarrer: wantBarrier{
					hit:  true,
					tick: 3,
				},
				handlerPhase: handler.EPhaseRunning,
				eventPhase:   core.TaskExecution_RUNNING,
				pluginPhase:  pluginCore.PhaseRunning,
				pluginVer:    1,
			},
		},
		{
			"barrier-trns-restart-case-ephemeral",
			args{
				prevTick: 2,
				res: []fakeplugins.HandleResponse{
					{T: pluginCore.DoTransitionType(pluginCore.TransitionTypeEphemeral, pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{}))},
				},
			},
			want{
				wantBarrer: wantBarrier{
					hit: false,
				},
				handlerPhase: handler.EPhaseRunning,
				eventPhase:   core.TaskExecution_RUNNING,
				pluginPhase:  pluginCore.PhaseRunning,
				pluginVer:    1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &taskNodeStateHolder{}
			ev := &fakeBufferedTaskEventRecorder{}
			nCtx := createNodeContext(ev, "test", state, tt.args.prevTick)
			c := &pluginCatalogMocks.Client{}

			tk, err := New(context.TODO(), mocks.NewFakeKubeClient(), c, promutils.NewTestScope())
			assert.NoError(t, err)

			tctx, err := tk.newTaskExecutionContext(context.TODO(), nCtx, "plugin1")
			assert.NoError(t, err)
			id := tctx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()

			if tt.args.bTrns != nil {
				x := &pluginRequestedTransition{}
				x.ObservedTransitionAndState(*tt.args.bTrns, 0, nil)
				tk.barrierCache.RecordBarrierTransition(context.TODO(), id, BarrierTransition{tt.args.btrnsTick, PluginCallLog{x}})
			}

			tk.plugins = map[pluginCore.TaskType]pluginCore.Plugin{
				"test": fakeplugins.NewReplayer("test", pluginCore.PluginProperties{},
					tt.args.res, nil, nil),
			}
			got, err := tk.Handle(context.TODO(), nCtx)
			if (err != nil) != tt.want.wantErr {
				t.Errorf("Handler.Handle() error = %v, wantErr %v", err, tt.want.wantErr)
				return
			}
			if err == nil {
				assert.Equal(t, tt.want.handlerPhase.String(), got.Info().GetPhase().String())
				if assert.Equal(t, 1, len(ev.evs)) {
					e := ev.evs[0]
					assert.Equal(t, tt.want.eventPhase.String(), e.Phase.String())
				}
				assert.Equal(t, tt.want.pluginPhase.String(), state.s.PluginPhase.String())
				assert.Equal(t, tt.want.pluginVer, state.s.PluginPhaseVersion)
				if tt.want.wantBarrer.hit {
					assert.Len(t, tk.barrierCache.barrierTransitions.Keys(), 1)
					bt := tk.barrierCache.GetPreviousBarrierTransition(context.TODO(), id)
					assert.Equal(t, bt.BarrierClockTick, tt.want.wantBarrer.tick)
					assert.Equal(t, tt.want.wantBarrer.tick, state.s.BarrierClockTick)
				} else {
					assert.Len(t, tk.barrierCache.barrierTransitions.Keys(), 0)
					assert.Equal(t, tt.args.prevTick, state.s.BarrierClockTick)
				}
			}
		})
	}
}

func Test_task_Abort(t *testing.T) {
	createNodeCtx := func(ev *fakeBufferedTaskEventRecorder) *nodeMocks.NodeExecutionContext {
		wfExecID := &core.WorkflowExecutionIdentifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
		}

		nm := &nodeMocks.NodeExecutionMetadata{}
		nm.On("GetAnnotations").Return(map[string]string{})
		nm.On("GetExecutionID").Return(v1alpha1.WorkflowExecutionIdentifier{
			WorkflowExecutionIdentifier: wfExecID,
		})
		nm.On("GetK8sServiceAccount").Return("service-account")
		nm.On("GetLabels").Return(map[string]string{})
		nm.On("GetNamespace").Return("namespace")
		nm.On("GetOwnerID").Return(types.NamespacedName{Namespace: "namespace", Name: "name"})
		nm.On("GetOwnerReference").Return(v12.OwnerReference{
			Kind: "sample",
			Name: "name",
		})

		taskID := &core.Identifier{}
		tr := &nodeMocks.TaskReader{}
		tr.On("GetTaskID").Return(taskID)
		tr.On("GetTaskType").Return("x")

		ns := &flyteMocks.ExecutableNodeStatus{}
		ns.On("GetDataDir").Return(storage.DataReference("data-dir"))

		res := &v1.ResourceRequirements{}
		n := &flyteMocks.ExecutableNode{}
		n.On("GetResources").Return(res)

		ir := &ioMocks.InputReader{}
		nCtx := &nodeMocks.NodeExecutionContext{}
		nCtx.On("NodeExecutionMetadata").Return(nm)
		nCtx.On("Node").Return(n)
		nCtx.On("InputReader").Return(ir)
		nCtx.On("DataStore").Return(storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope()))
		nCtx.On("CurrentAttempt").Return(uint32(1))
		nCtx.On("TaskReader").Return(tr)
		nCtx.On("MaxDatasetSizeBytes").Return(int64(1))
		nCtx.On("NodeStatus").Return(ns)
		nCtx.On("NodeID").Return("n1")
		nCtx.On("EnqueueOwner").Return(nil)
		nCtx.On("EventsRecorder").Return(ev)

		st := bytes.NewBuffer([]byte{})
		a := 45
		type test struct {
			A int
		}
		cod := codex.GobStateCodec{}
		assert.NoError(t, cod.Encode(test{A: a}, st))
		nr := &nodeMocks.NodeStateReader{}
		nr.On("GetTaskNodeState").Return(handler.TaskNodeState{
			PluginState: st.Bytes(),
		})
		nCtx.On("NodeStateReader").Return(nr)
		return nCtx
	}

	type fields struct {
		defaultPluginCallback func() pluginCore.Plugin
	}
	type args struct {
		ev *fakeBufferedTaskEventRecorder
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		abortCalled bool
	}{
		{"no-plugin", fields{defaultPluginCallback: func() pluginCore.Plugin {
			return nil
		}}, args{nil}, true, false},

		{"abort-fails", fields{defaultPluginCallback: func() pluginCore.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("GetID").Return("id")
			p.On("Abort", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
			return p
		}}, args{nil}, true, true},
		{"abort-success", fields{defaultPluginCallback: func() pluginCore.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("GetID").Return("id")
			p.On("Abort", mock.Anything, mock.Anything).Return(nil)
			return p
		}}, args{ev: &fakeBufferedTaskEventRecorder{}}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.fields.defaultPluginCallback()
			tk := Handler{
				defaultPlugin: m,
			}
			nCtx := createNodeCtx(tt.args.ev)
			if err := tk.Abort(context.TODO(), nCtx, "reason"); (err != nil) != tt.wantErr {
				t.Errorf("Handler.Abort() error = %v, wantErr %v", err, tt.wantErr)
			}
			c := 0
			if tt.abortCalled {
				c = 1
				if !tt.wantErr {
					assert.Len(t, tt.args.ev.evs, 1)
				}
			}
			if m != nil {
				m.(*pluginCoreMocks.Plugin).AssertNumberOfCalls(t, "Abort", c)
			}
		})
	}
}

func Test_task_Finalize(t *testing.T) {

	wfExecID := &core.WorkflowExecutionIdentifier{
		Project: "project",
		Domain:  "domain",
		Name:    "name",
	}

	nm := &nodeMocks.NodeExecutionMetadata{}
	nm.On("GetAnnotations").Return(map[string]string{})
	nm.On("GetExecutionID").Return(v1alpha1.WorkflowExecutionIdentifier{
		WorkflowExecutionIdentifier: wfExecID,
	})
	nm.On("GetK8sServiceAccount").Return("service-account")
	nm.On("GetLabels").Return(map[string]string{})
	nm.On("GetNamespace").Return("namespace")
	nm.On("GetOwnerID").Return(types.NamespacedName{Namespace: "namespace", Name: "name"})
	nm.On("GetOwnerReference").Return(v12.OwnerReference{
		Kind: "sample",
		Name: "name",
	})

	taskID := &core.Identifier{}
	tr := &nodeMocks.TaskReader{}
	tr.On("GetTaskID").Return(taskID)
	tr.On("GetTaskType").Return("x")

	ns := &flyteMocks.ExecutableNodeStatus{}
	ns.On("GetDataDir").Return(storage.DataReference("data-dir"))

	res := &v1.ResourceRequirements{}
	n := &flyteMocks.ExecutableNode{}
	n.On("GetResources").Return(res)

	ir := &ioMocks.InputReader{}
	nCtx := &nodeMocks.NodeExecutionContext{}
	nCtx.On("NodeExecutionMetadata").Return(nm)
	nCtx.On("Node").Return(n)
	nCtx.On("InputReader").Return(ir)
	nCtx.On("DataStore").Return(storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope()))
	nCtx.On("CurrentAttempt").Return(uint32(1))
	nCtx.On("TaskReader").Return(tr)
	nCtx.On("MaxDatasetSizeBytes").Return(int64(1))
	nCtx.On("NodeStatus").Return(ns)
	nCtx.On("NodeID").Return("n1")
	nCtx.On("EventsRecorder").Return(nil)
	nCtx.On("EnqueueOwner").Return(nil)

	st := bytes.NewBuffer([]byte{})
	a := 45
	type test struct {
		A int
	}
	cod := codex.GobStateCodec{}
	assert.NoError(t, cod.Encode(test{A: a}, st))
	nr := &nodeMocks.NodeStateReader{}
	nr.On("GetTaskNodeState").Return(handler.TaskNodeState{
		PluginState: st.Bytes(),
	})
	nCtx.On("NodeStateReader").Return(nr)
	type fields struct {
		defaultPluginCallback func() pluginCore.Plugin
	}
	type args struct {
		nCtx handler.NodeExecutionContext
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantErr  bool
		finalize bool
	}{
		{"no-plugin", fields{defaultPluginCallback: func() pluginCore.Plugin {
			return nil
		}}, args{nCtx: nCtx}, true, false},

		{"finalize-fails", fields{defaultPluginCallback: func() pluginCore.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("GetID").Return("id")
			p.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
			return p
		}}, args{nCtx: nCtx}, true, true},
		{"finalize-success", fields{defaultPluginCallback: func() pluginCore.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("GetID").Return("id")
			p.On("Finalize", mock.Anything, mock.Anything).Return(nil)
			return p
		}}, args{nCtx: nCtx}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.fields.defaultPluginCallback()
			tk := Handler{
				defaultPlugin: m,
			}
			if err := tk.Finalize(context.TODO(), tt.args.nCtx); (err != nil) != tt.wantErr {
				t.Errorf("Handler.Finalize() error = %v, wantErr %v", err, tt.wantErr)
			}
			c := 0
			if tt.finalize {
				c = 1
			}
			if m != nil {
				m.(*pluginCoreMocks.Plugin).AssertNumberOfCalls(t, "Finalize", c)
			}
		})
	}
}

func TestNew(t *testing.T) {
	got, err := New(context.TODO(), mocks.NewFakeKubeClient(), &pluginCatalogMocks.Client{}, promutils.NewTestScope())
	assert.NoError(t, err)
	assert.NotNil(t, got)
	assert.NotNil(t, got.plugins)
	assert.NotNil(t, got.metrics)
	assert.Equal(t, got.pluginRegistry, pluginmachinery.PluginRegistry())
}

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey,
		contextutils.TaskIDKey)
}
