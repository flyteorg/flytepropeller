package task

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginCoreMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	pluginK8s "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	pluginK8sMocks "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s/mocks"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flytepropeller/pkg/controller/executors/mocks"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	nodeMocks "github.com/lyft/flytepropeller/pkg/controller/nodes/handler/mocks"
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
			core: []pluginCore.PluginEntry{corePluginEntry}, k8s: []pluginK8s.PluginEntry{k8sPluginEntry, k8sPluginEntryDefault},
		}, wantFields{
			pluginIDs:       map[pluginCore.TaskType]string{corePluginType: corePluginType, k8sPluginType: k8sPluginType, k8sPluginDefaultType: k8sPluginDefaultType},
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

			tk := New(context.TODO(), promutils.NewTestScope())
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

func Test_task_Handle(t *testing.T) {
	type fields struct {
		ttype  pluginCore.TaskType
		plugin pluginCore.Plugin
	}
	type args struct {
		nCtx handler.NodeExecutionContext
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    handler.Transition
		wantErr bool
	}{
		{
			// "immediate-success", fields{
			// ttype:  "test",
			// plugin: nil,
			// },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tk := Handler{
				plugins: map[pluginCore.TaskType]pluginCore.Plugin{
					tt.fields.ttype: tt.fields.plugin,
				},
			}
			got, err := tk.Handle(context.TODO(), tt.args.nCtx)
			if (err != nil) != tt.wantErr {
				t.Errorf("Handler.Handle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Handler.Handle() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_task_Abort(t *testing.T) {
	type fields struct {
		defaultPluginCallback func() *pluginCoreMocks.Plugin
	}
	type args struct {
		nCtx handler.NodeExecutionContext
	}

	nCtx := &nodeMocks.NodeExecutionContext{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		abortCalled bool
	}{
		{"no-plugin", fields{defaultPluginCallback: func() *pluginCoreMocks.Plugin {
			return nil
		}}, args{nCtx: nCtx}, true, false},

		{"abort-fails", fields{defaultPluginCallback: func() *pluginCoreMocks.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("Abort", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
			return p
		}}, args{nCtx: nCtx}, true, true},
		{"abort-success", fields{defaultPluginCallback: func() *pluginCoreMocks.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("Abort", mock.Anything, mock.Anything).Return(nil)
			return p
		}}, args{nCtx: nCtx}, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.fields.defaultPluginCallback()
			tk := Handler{
				defaultPlugin: m,
			}
			if err := tk.Abort(context.TODO(), tt.args.nCtx); (err != nil) != tt.wantErr {
				t.Errorf("Handler.Abort() error = %v, wantErr %v", err, tt.wantErr)
			}
			c := 0
			if tt.abortCalled {
				c = 1
			}
			m.AssertNumberOfCalls(t, "Abort", c)
		})
	}
}

func Test_task_Finalize(t *testing.T) {
	type fields struct {
		defaultPluginCallback func() *pluginCoreMocks.Plugin
	}
	type args struct {
		nCtx handler.NodeExecutionContext
	}
	nCtx := &nodeMocks.NodeExecutionContext{}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantErr  bool
		finalize bool
	}{
		{"no-plugin", fields{defaultPluginCallback: func() *pluginCoreMocks.Plugin {
			return nil
		}}, args{nCtx: nCtx}, true, false},

		{"finalize-fails", fields{defaultPluginCallback: func() *pluginCoreMocks.Plugin {
			p := &pluginCoreMocks.Plugin{}
			p.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
			return p
		}}, args{nCtx: nCtx}, true, true},
		{"finalize-success", fields{defaultPluginCallback: func() *pluginCoreMocks.Plugin {
			p := &pluginCoreMocks.Plugin{}
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
			m.AssertNumberOfCalls(t, "Finalize", c)
		})
	}
}

func TestNew(t *testing.T) {
	got := New(context.TODO(), promutils.NewTestScope())
	assert.NotNil(t, got)
	assert.NotNil(t, got.plugins)
	assert.NotNil(t, got.metrics)
	assert.Equal(t, got.pluginRegistry, pluginmachinery.PluginRegistry())
}
