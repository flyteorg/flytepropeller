package nodes

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/admin"
	mocks3 "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io/mocks"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/lyft/flytestdlib/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1/mocks"
	mocks4 "github.com/lyft/flytepropeller/pkg/controller/executors/mocks"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	nodeHandlerMocks "github.com/lyft/flytepropeller/pkg/controller/nodes/handler/mocks"
	mocks2 "github.com/lyft/flytepropeller/pkg/controller/nodes/mocks"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/catalog"

	"github.com/lyft/flyteidl/clients/go/events"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/config"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/lyft/flytepropeller/pkg/utils"
	flyteassert "github.com/lyft/flytepropeller/pkg/utils/assert"
)

var fakeKubeClient = mocks4.NewFakeKubeClient()
var catalogClient = catalog.NOOPCatalog{}

const taskID = "tID"

func TestSetInputsForStartNode(t *testing.T) {
	ctx := context.Background()
	mockStorage := createInmemoryDataStore(t, testScope.NewSubScope("f"))
	enQWf := func(workflowID v1alpha1.WorkflowID) {}

	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	exec, err := NewExecutor(ctx, config.GetConfig().NodeConfig, mockStorage, enQWf, events.NewMockEventSink(), adminClient,
		adminClient, 10, "s3://bucket/", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	inputs := &core.LiteralMap{
		Literals: map[string]*core.Literal{
			"x": utils.MustMakePrimitiveLiteral("hello"),
			"y": utils.MustMakePrimitiveLiteral("blah"),
		},
	}

	t.Run("NoInputs", func(t *testing.T) {
		w := createDummyBaseWorkflow(mockStorage)
		w.DummyStartNode = &v1alpha1.NodeSpec{
			ID: v1alpha1.StartNodeID,
		}
		s, err := exec.SetInputsForStartNode(ctx, w, w, w, nil)
		assert.NoError(t, err)
		assert.Equal(t, executors.NodeStatusComplete, s)
	})

	t.Run("WithInputs", func(t *testing.T) {
		w := createDummyBaseWorkflow(mockStorage)
		w.GetNodeExecutionStatus(ctx, v1alpha1.StartNodeID).SetDataDir("s3://test-bucket/exec/start-node/data")
		w.GetNodeExecutionStatus(ctx, v1alpha1.StartNodeID).SetOutputDir("s3://test-bucket/exec/start-node/data/0")
		w.DummyStartNode = &v1alpha1.NodeSpec{
			ID: v1alpha1.StartNodeID,
		}
		s, err := exec.SetInputsForStartNode(ctx, w, w, w, inputs)
		assert.NoError(t, err)
		assert.Equal(t, executors.NodeStatusComplete, s)
		actual := &core.LiteralMap{}
		if assert.NoError(t, mockStorage.ReadProtobuf(ctx, "s3://test-bucket/exec/start-node/data/0/outputs.pb", actual)) {
			flyteassert.EqualLiteralMap(t, inputs, actual)
		}
	})

	t.Run("DataDirNotSet", func(t *testing.T) {
		w := createDummyBaseWorkflow(mockStorage)
		w.DummyStartNode = &v1alpha1.NodeSpec{
			ID: v1alpha1.StartNodeID,
		}
		s, err := exec.SetInputsForStartNode(ctx, w, w, w, inputs)
		assert.Error(t, err)
		assert.Equal(t, executors.NodeStatusUndefined, s)
	})

	failStorage := createFailingDatastore(t, testScope.NewSubScope("failing"))
	execFail, err := NewExecutor(ctx, config.GetConfig().NodeConfig, failStorage, enQWf, events.NewMockEventSink(), adminClient,
		adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	t.Run("StorageFailure", func(t *testing.T) {
		w := createDummyBaseWorkflow(mockStorage)
		w.GetNodeExecutionStatus(ctx, v1alpha1.StartNodeID).SetDataDir("s3://test-bucket/exec/start-node/data")
		w.DummyStartNode = &v1alpha1.NodeSpec{
			ID: v1alpha1.StartNodeID,
		}
		s, err := execFail.SetInputsForStartNode(ctx, w, w, w, inputs)
		assert.Error(t, err)
		assert.Equal(t, executors.NodeStatusUndefined, s)
	})
}

func TestNodeExecutor_Initialize(t *testing.T) {
	ctx := context.Background()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}

	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)
	memStore, err := storage.NewDataStore(&storage.Config{Type: storage.TypeMemory}, promutils.NewTestScope())
	assert.NoError(t, err)
	adminClient := launchplan.NewFailFastLaunchPlanExecutor()

	t.Run("happy", func(t *testing.T) {
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, memStore, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)

		hf := &mocks2.HandlerFactory{}
		exec.nodeHandlerFactory = hf

		hf.On("Setup", mock.Anything, mock.Anything).Return(nil)

		assert.NoError(t, exec.Initialize(ctx))
	})

	t.Run("error", func(t *testing.T) {
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, memStore, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)

		hf := &mocks2.HandlerFactory{}
		exec.nodeHandlerFactory = hf

		hf.On("Setup", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))

		assert.Error(t, exec.Initialize(ctx))
	})
}

func TestNodeExecutor_RecursiveNodeHandler_RecurseStartNodes(t *testing.T) {
	ctx := context.Background()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}
	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)

	store := createInmemoryDataStore(t, promutils.NewTestScope())

	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient, adminClient,
		10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	exec := execIface.(*nodeExecutor)

	defaultNodeID := "n1"

	createStartNodeWf := func(p v1alpha1.NodePhase, _ int) (v1alpha1.ExecutableWorkflow, v1alpha1.ExecutableNode, v1alpha1.ExecutableNodeStatus) {
		startNode := &v1alpha1.NodeSpec{
			Kind: v1alpha1.NodeKindStart,
			ID:   v1alpha1.StartNodeID,
		}
		startNodeStatus := &v1alpha1.NodeStatus{
			Phase: p,
		}
		return &v1alpha1.FlyteWorkflow{
			Status: v1alpha1.WorkflowStatus{
				NodeStatus: map[v1alpha1.NodeID]*v1alpha1.NodeStatus{
					v1alpha1.StartNodeID: startNodeStatus,
				},
				DataDir: "data",
			},
			WorkflowSpec: &v1alpha1.WorkflowSpec{
				ID: "wf",
				Nodes: map[v1alpha1.NodeID]*v1alpha1.NodeSpec{
					v1alpha1.StartNodeID: startNode,
				},
				Connections: v1alpha1.Connections{
					UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						defaultNodeID: {v1alpha1.StartNodeID},
					},
					DownstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						v1alpha1.StartNodeID: {defaultNodeID},
					},
				},
			},
			DataReferenceConstructor: store,
			RawOutputDataConfig: v1alpha1.RawOutputDataConfig{
				RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
			},
		}, startNode, startNodeStatus

	}

	// Recurse Child Node Queued previously
	{
		tests := []struct {
			name              string
			currentNodePhase  v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			handlerReturn     func() (handler.Transition, error)
			expectedError     bool
		}{
			// Starting at Queued
			{"nys->success", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseSucceeded, executors.NodePhaseSuccess, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
			}, false},
			{"queued->success", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseSucceeded, executors.NodePhaseSuccess, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
			}, false},
			{"nys->error", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseNotYetStarted, executors.NodePhaseUndefined, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("err")
			}, true},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				exec.nodeHandlerFactory = hf

				h := &nodeHandlerMocks.Node{}
				h.On("Handle",
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(test.handlerReturn())
				h.On("FinalizeRequired").Return(false)

				hf.On("GetHandler", v1alpha1.NodeKindStart).Return(h, nil)

				mockWf, startNode, startNodeStatus := createStartNodeWf(test.currentNodePhase, 0)
				executionContext := executors.NewExecutionContext(mockWf, nil, nil, nil)
				s, err := exec.RecursiveNodeHandler(ctx, executionContext, mockWf, mockWf, startNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, uint32(0), startNodeStatus.GetAttempts())
				assert.Equal(t, test.expectedNodePhase, startNodeStatus.GetPhase(), "expected %s, received %s", test.expectedNodePhase.String(), startNodeStatus.GetPhase().String())
			})
		}
	}
}

func TestNodeExecutor_RecursiveNodeHandler_RecurseEndNode(t *testing.T) {
	ctx := context.Background()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}
	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)

	store := createInmemoryDataStore(t, promutils.NewTestScope())

	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient, adminClient,
		10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	exec := execIface.(*nodeExecutor)

	// Node not yet started
	{
		createSingleNodeWf := func(parentPhase v1alpha1.NodePhase, _ int) (v1alpha1.ExecutableWorkflow, v1alpha1.ExecutableNode, v1alpha1.ExecutableNodeStatus) {
			n := &v1alpha1.NodeSpec{
				ID:   v1alpha1.EndNodeID,
				Kind: v1alpha1.NodeKindEnd,
			}
			ns := &v1alpha1.NodeStatus{}

			return &v1alpha1.FlyteWorkflow{
				Status: v1alpha1.WorkflowStatus{
					NodeStatus: map[v1alpha1.NodeID]*v1alpha1.NodeStatus{
						v1alpha1.EndNodeID: ns,
						v1alpha1.StartNodeID: {
							Phase: parentPhase,
						},
					},
					DataDir: "wf-data",
				},
				WorkflowSpec: &v1alpha1.WorkflowSpec{
					ID: "wf",
					Nodes: map[v1alpha1.NodeID]*v1alpha1.NodeSpec{
						v1alpha1.EndNodeID: n,
					},
					Connections: v1alpha1.Connections{
						UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
							v1alpha1.EndNodeID: {v1alpha1.StartNodeID},
						},
					},
				},
				DataReferenceConstructor: store,
				RawOutputDataConfig: v1alpha1.RawOutputDataConfig{
					RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
				},
			}, n, ns

		}
		tests := []struct {
			name              string
			parentNodePhase   v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			expectedError     bool
		}{
			{"notYetStarted", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"running", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"queued", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"retryable", v1alpha1.NodePhaseRetryableFailure, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"failing", v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"skipped", v1alpha1.NodePhaseSkipped, v1alpha1.NodePhaseSkipped, executors.NodePhaseSuccess, false},
			{"success", v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseQueued, executors.NodePhaseQueued, false},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				exec.nodeHandlerFactory = hf
				h := &nodeHandlerMocks.Node{}
				hf.On("GetHandler", v1alpha1.NodeKindEnd).Return(h, nil)

				mockWf, mockNode, mockNodeStatus := createSingleNodeWf(test.parentNodePhase, 0)
				execContext := executors.NewExecutionContext(mockWf, nil, nil, nil)
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, mockNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
				assert.Equal(t, test.expectedNodePhase, mockNodeStatus.GetPhase(), "expected %s, received %s", test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String())

				if test.expectedNodePhase == v1alpha1.NodePhaseQueued {
					assert.Equal(t, mockNodeStatus.GetDataDir(), storage.DataReference("/wf-data/end-node/data"))
					assert.Equal(t, mockNodeStatus.GetOutputDir(), storage.DataReference("/wf-data/end-node/data/0"))
				}
			})
		}
	}

	// Recurse End Node Queued previously
	{
		createSingleNodeWf := func(endNodePhase v1alpha1.NodePhase, _ int) (v1alpha1.ExecutableWorkflow, executors.ExecutionContext, v1alpha1.ExecutableNode, v1alpha1.ExecutableNodeStatus) {
			n := &v1alpha1.NodeSpec{
				ID:   v1alpha1.EndNodeID,
				Kind: v1alpha1.NodeKindEnd,
			}
			ns := &v1alpha1.NodeStatus{
				Phase:                endNodePhase,
				LastAttemptStartedAt: &v1.Time{},
			}

			w := &v1alpha1.FlyteWorkflow{
				Status: v1alpha1.WorkflowStatus{
					NodeStatus: map[v1alpha1.NodeID]*v1alpha1.NodeStatus{
						v1alpha1.EndNodeID: ns,
						v1alpha1.StartNodeID: {
							Phase: v1alpha1.NodePhaseSucceeded,
						},
					},
					DataDir: "data",
				},
				WorkflowSpec: &v1alpha1.WorkflowSpec{
					ID: "wf",
					Nodes: map[v1alpha1.NodeID]*v1alpha1.NodeSpec{
						v1alpha1.StartNodeID: {
							ID:   v1alpha1.StartNodeID,
							Kind: v1alpha1.NodeKindStart,
						},
						v1alpha1.EndNodeID: n,
					},
					Connections: v1alpha1.Connections{
						UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
							v1alpha1.EndNodeID: {v1alpha1.StartNodeID},
						},
						DownstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
							v1alpha1.StartNodeID: {v1alpha1.EndNodeID},
						},
					},
				},
				DataReferenceConstructor: store,
				RawOutputDataConfig: v1alpha1.RawOutputDataConfig{
					RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
				},
			}
			executionContext := executors.NewExecutionContext(w, nil, nil, nil)
			return w, executionContext, n, ns
		}

		tests := []struct {
			name              string
			currentNodePhase  v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			handlerReturn     func() (handler.Transition, error)
			expectedError     bool
		}{
			// Starting at Queued
			{"queued->success", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseSucceeded, executors.NodePhaseSuccess, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
			}, false},

			{"queued->failed", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseFailed, executors.NodePhaseFailed, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_USER, "code", "mesage", nil)), nil
			}, false},

			{"queued->running", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseRunning, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
			}, false},

			{"queued->error", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseQueued, executors.NodePhaseUndefined, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("err")
			}, true},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				exec.nodeHandlerFactory = hf

				h := &nodeHandlerMocks.Node{}
				h.On("Handle",
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(test.handlerReturn())
				h.OnFinalizeRequired().Return(false)

				hf.OnGetHandler(v1alpha1.NodeKindEnd).Return(h, nil)

				mockWf, execContext, _, mockNodeStatus := createSingleNodeWf(test.currentNodePhase, 0)
				startNode := mockWf.StartNode()
				startStatus := mockWf.GetNodeExecutionStatus(ctx, startNode.GetID())
				assert.Equal(t, v1alpha1.NodePhaseSucceeded, startStatus.GetPhase())
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				if test.expectedPhase == executors.NodePhaseFailed {
					assert.NotNil(t, s.Err)
				} else {
					assert.Nil(t, s.Err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
				assert.Equal(t, test.expectedNodePhase, mockNodeStatus.GetPhase(), "expected %s, received %s", test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String())
			})
		}
	}
}

func TestNodeExecutor_RecursiveNodeHandler_Recurse(t *testing.T) {
	ctx := context.Background()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}
	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)

	defaultNodeID := "n1"
	taskID := taskID

	store := createInmemoryDataStore(t, promutils.NewTestScope())
	createSingleNodeWf := func(p v1alpha1.NodePhase, maxAttempts int) (v1alpha1.ExecutableWorkflow, v1alpha1.ExecutableNode, v1alpha1.ExecutableNodeStatus) {
		n := &v1alpha1.NodeSpec{
			ID:      defaultNodeID,
			TaskRef: &taskID,
			Kind:    v1alpha1.NodeKindTask,
			RetryStrategy: &v1alpha1.RetryStrategy{
				MinAttempts: &maxAttempts,
			},
		}

		var err *v1alpha1.ExecutionError
		if p == v1alpha1.NodePhaseFailing || p == v1alpha1.NodePhaseFailed || p == v1alpha1.NodePhaseRetryableFailure {
			err = &v1alpha1.ExecutionError{ExecutionError: &core.ExecutionError{Code: "test", Message: "test"}}
		}
		ns := &v1alpha1.NodeStatus{
			Phase:                p,
			LastAttemptStartedAt: &v1.Time{},
			Error:                err,
		}

		startNode := &v1alpha1.NodeSpec{
			Kind: v1alpha1.NodeKindStart,
			ID:   v1alpha1.StartNodeID,
		}
		return &v1alpha1.FlyteWorkflow{
			Tasks: map[v1alpha1.TaskID]*v1alpha1.TaskSpec{
				taskID: {
					TaskTemplate: &core.TaskTemplate{},
				},
			},
			Status: v1alpha1.WorkflowStatus{
				NodeStatus: map[v1alpha1.NodeID]*v1alpha1.NodeStatus{
					defaultNodeID: ns,
					v1alpha1.StartNodeID: {
						Phase: v1alpha1.NodePhaseSucceeded,
					},
				},
				DataDir: "data",
			},
			WorkflowSpec: &v1alpha1.WorkflowSpec{
				ID: "wf",
				Nodes: map[v1alpha1.NodeID]*v1alpha1.NodeSpec{
					defaultNodeID:        n,
					v1alpha1.StartNodeID: startNode,
				},
				Connections: v1alpha1.Connections{
					UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						defaultNodeID: {v1alpha1.StartNodeID},
					},
					DownstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						v1alpha1.StartNodeID: {defaultNodeID},
					},
				},
			},
			DataReferenceConstructor: store,
			RawOutputDataConfig: v1alpha1.RawOutputDataConfig{
				RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
			},
		}, n, ns

	}

	// Recursion test with child Node not yet started
	t.Run("ChildNodeNotYetStarted", func(t *testing.T) {
		nodeN0 := "n0"
		nodeN2 := "n2"
		ctx := context.Background()
		connections := &v1alpha1.Connections{
			UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
				nodeN2: {nodeN0},
			},
		}

		setupNodePhase := func(n0Phase, n2Phase, expectedN2Phase v1alpha1.NodePhase) (*mocks.ExecutableWorkflow, *mocks.ExecutableNodeStatus) {
			taskID := "id"
			taskID0 := "id1"
			// Setup
			mockN2Status := &mocks.ExecutableNodeStatus{}
			// No parent node
			mockN2Status.OnGetParentNodeID().Return(nil)
			mockN2Status.OnGetParentTaskID().Return(nil)
			mockN2Status.OnGetPhase().Return(n2Phase)
			mockN2Status.On("SetDataDir", mock.AnythingOfType(reflect.TypeOf(storage.DataReference("x")).String()))
			mockN2Status.OnGetDataDir().Return(storage.DataReference("blah"))
			mockN2Status.On("SetOutputDir", mock.AnythingOfType(reflect.TypeOf(storage.DataReference("x")).String()))
			mockN2Status.OnGetOutputDir().Return(storage.DataReference("blah"))
			mockN2Status.OnGetWorkflowNodeStatus().Return(nil)

			mockN2Status.OnGetStoppedAt().Return(nil)
			var ee *core.ExecutionError
			mockN2Status.On("UpdatePhase", expectedN2Phase, mock.Anything, mock.AnythingOfType("string"), ee)
			mockN2Status.OnIsDirty().Return(false)
			mockN2Status.OnGetTaskNodeStatus().Return(nil)
			mockN2Status.On("ClearDynamicNodeStatus").Return(nil)
			mockN2Status.OnGetAttempts().Return(uint32(0))
			if expectedN2Phase == v1alpha1.NodePhaseFailed {
				mockN2Status.OnGetExecutionError().Return(&core.ExecutionError{
					Message: "Expected Failure",
				})
			}

			mockNode := &mocks.ExecutableNode{}
			mockNode.OnGetID().Return(nodeN2)
			mockNode.OnGetBranchNode().Return(nil)
			mockNode.OnGetKind().Return(v1alpha1.NodeKindTask)
			mockNode.OnIsStartNode().Return(false)
			mockNode.OnIsEndNode().Return(false)
			mockNode.OnGetTaskID().Return(&taskID)
			mockNode.OnGetInputBindings().Return([]*v1alpha1.Binding{})
			mockNode.OnIsInterruptible().Return(nil)
			mockNode.OnGetName().Return("name")

			mockNodeN0 := &mocks.ExecutableNode{}
			mockNodeN0.OnGetID().Return(nodeN0)
			mockNodeN0.OnGetBranchNode().Return(nil)
			mockNodeN0.OnGetKind().Return(v1alpha1.NodeKindTask)
			mockNodeN0.OnIsStartNode().Return(false)
			mockNodeN0.OnIsEndNode().Return(false)
			mockNodeN0.OnGetTaskID().Return(&taskID0)
			mockNodeN0.OnIsInterruptible().Return(nil)
			mockNodeN0.OnGetName().Return("name")

			mockN0Status := &mocks.ExecutableNodeStatus{}
			mockN0Status.OnGetPhase().Return(n0Phase)
			mockN0Status.OnGetAttempts().Return(uint32(0))
			mockN0Status.OnGetExecutionError().Return(nil)

			mockN0Status.OnIsDirty().Return(false)
			mockN0Status.OnGetParentTaskID().Return(nil)
			n := v1.Now()
			mockN0Status.OnGetStoppedAt().Return(&n)

			tk := &mocks.ExecutableTask{}
			tk.OnCoreTask().Return(&core.TaskTemplate{})
			mockWfStatus := &mocks.ExecutableWorkflowStatus{}
			mockWf := &mocks.ExecutableWorkflow{}
			mockWf.OnStartNode().Return(mockNodeN0)
			mockWf.OnGetNode(nodeN2).Return(mockNode, true)
			mockWf.OnGetNodeExecutionStatusMatch(mock.Anything, nodeN0).Return(mockN0Status)
			mockWf.OnGetNodeExecutionStatusMatch(mock.Anything, nodeN2).Return(mockN2Status)
			mockWf.OnGetConnections().Return(connections)
			mockWf.OnGetID().Return("w1")
			mockWf.OnToNode(nodeN2).Return([]string{nodeN0}, nil)
			mockWf.OnFromNode(nodeN0).Return([]string{nodeN2}, nil)
			mockWf.OnFromNode(nodeN2).Return([]string{}, fmt.Errorf("did not expect"))
			mockWf.OnGetExecutionID().Return(v1alpha1.WorkflowExecutionIdentifier{})
			mockWf.OnGetExecutionStatus().Return(mockWfStatus)
			mockWf.OnGetTask(taskID0).Return(tk, nil)
			mockWf.OnGetTask(taskID).Return(tk, nil)
			mockWf.OnGetLabels().Return(make(map[string]string))
			mockWf.OnIsInterruptible().Return(false)
			mockWf.OnGetEventVersion().Return(v1alpha1.EventVersion0)
			mockWf.OnGetOnFailurePolicy().Return(v1alpha1.WorkflowOnFailurePolicy(core.WorkflowMetadata_FAIL_IMMEDIATELY))
			mockWf.OnGetRawOutputDataConfig().Return(v1alpha1.RawOutputDataConfig{
				RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
			})
			mockWfStatus.OnGetDataDir().Return(storage.DataReference("x"))
			mockWfStatus.OnConstructNodeDataDirMatch(mock.Anything, mock.Anything, mock.Anything).Return("x", nil)
			return mockWf, mockN2Status
		}

		tests := []struct {
			name              string
			currentNodePhase  v1alpha1.NodePhase
			parentNodePhase   v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			expectedError     bool
			updateCalled      bool
		}{
			{"notYetStarted->skipped", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseFailed, v1alpha1.NodePhaseSkipped, executors.NodePhaseFailed, false, false},
			{"notYetStarted->skipped", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseSkipped, v1alpha1.NodePhaseSkipped, executors.NodePhaseSuccess, false, true},
			{"notYetStarted->queued", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseQueued, executors.NodePhasePending, false, true},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}

				h := &nodeHandlerMocks.Node{}
				h.OnHandleMatch(
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(handler.UnknownTransition, fmt.Errorf("should not be called"))
				h.On("FinalizeRequired").Return(false)
				hf.On("GetHandler", v1alpha1.NodeKindTask).Return(h, nil)

				mockWf, _ := setupNodePhase(test.parentNodePhase, test.currentNodePhase, test.expectedNodePhase)
				startNode := mockWf.StartNode()
				store := createInmemoryDataStore(t, promutils.NewTestScope())

				adminClient := launchplan.NewFailFastLaunchPlanExecutor()
				execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink,
					adminClient, adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
				assert.NoError(t, err)
				exec := execIface.(*nodeExecutor)
				exec.nodeHandlerFactory = hf

				execContext := executors.NewExecutionContext(mockWf, mockWf, mockWf, nil)
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
			})
		}
	})

	// Recurse Child Node Queued previously
	t.Run("ChildNodeQueuedPreviously", func(t *testing.T) {
		tests := []struct {
			name              string
			currentNodePhase  v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			handlerReturn     func() (handler.Transition, error)
			finalizeReturnErr bool
			expectedError     bool
			eventRecorded     bool
			eventPhase        core.NodeExecution_Phase
		}{
			// Starting at Queued
			{"queued->running", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseRunning, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
			}, true, false, true, core.NodeExecution_RUNNING},

			{"queued->queued", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseQueued, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoQueued("reason")), nil
			}, true, false, false, core.NodeExecution_QUEUED},

			{"queued->failing", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseFailing, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_USER, "code", "reason", nil)), nil
			}, true, false, true, core.NodeExecution_FAILED},

			{"failing->failed", v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseFailed, executors.NodePhaseFailed, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, false, false, false, core.NodeExecution_FAILED},

			{"failing->failed(error)", v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseFailing, executors.NodePhaseUndefined, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, true, true, false, core.NodeExecution_FAILING},

			{"queued->succeeding", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseSucceeding, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
			}, true, false, true, core.NodeExecution_SUCCEEDED},

			{"succeeding->success", v1alpha1.NodePhaseSucceeding, v1alpha1.NodePhaseSucceeded, executors.NodePhaseSuccess, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, false, false, false, core.NodeExecution_SUCCEEDED},

			{"succeeding->success(error)", v1alpha1.NodePhaseSucceeding, v1alpha1.NodePhaseSucceeding, executors.NodePhaseUndefined, func() (handler.Transition, error) {

				return handler.UnknownTransition, fmt.Errorf("error")
			}, true, true, false, core.NodeExecution_SUCCEEDED},

			{"queued->error", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseQueued, executors.NodePhaseUndefined, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, true, true, false, core.NodeExecution_RUNNING},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}

				store := createInmemoryDataStore(t, promutils.NewTestScope())
				adminClient := launchplan.NewFailFastLaunchPlanExecutor()
				execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
					adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
				assert.NoError(t, err)
				exec := execIface.(*nodeExecutor)
				exec.nodeHandlerFactory = hf

				called := false
				exec.nodeRecorder = &events.MockRecorder{
					RecordNodeEventCb: func(ctx context.Context, ev *event.NodeExecutionEvent) error {
						assert.NotNil(t, ev)
						assert.Equal(t, test.eventPhase, ev.Phase)
						called = true
						return nil
					},
				}

				h := &nodeHandlerMocks.Node{}
				h.On("Handle",
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(test.handlerReturn())
				h.On("FinalizeRequired").Return(true)

				if test.finalizeReturnErr {
					h.OnFinalizeMatch(mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
				} else {
					h.OnFinalizeMatch(mock.Anything, mock.Anything).Return(nil)
				}
				hf.OnGetHandler(v1alpha1.NodeKindTask).Return(h, nil)

				mockWf, _, mockNodeStatus := createSingleNodeWf(test.currentNodePhase, 0)
				execErr := mockNodeStatus.GetExecutionError()
				startNode := mockWf.StartNode()
				startStatus := mockWf.GetNodeExecutionStatus(ctx, startNode.GetID())
				assert.Equal(t, v1alpha1.NodePhaseSucceeded, startStatus.GetPhase())
				execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, test.expectedNodePhase, mockNodeStatus.GetPhase(), "expected %s, received %s", test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String())
				if test.expectedNodePhase == v1alpha1.NodePhaseFailing {
					assert.NotNil(t, mockNodeStatus.GetExecutionError())
				} else if test.expectedNodePhase == v1alpha1.NodePhaseFailed {
					assert.NotNil(t, s.Err)
					assert.Equal(t, execErr, s.Err)
				} else {
					assert.Nil(t, s.Err)
				}
				assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
				assert.Equal(t, test.eventRecorded, called, "event recording expected: %v, but got %v", test.eventRecorded, called)
			})
		}
	})

	// Recurse Child Node started previously
	t.Run("ChildNodeStartedPreviously", func(t *testing.T) {
		tests := []struct {
			name              string
			currentNodePhase  v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			handlerReturn     func() (handler.Transition, error)
			expectedError     bool
			eventRecorded     bool
			eventPhase        core.NodeExecution_Phase
			attempts          int
		}{
			{"running->running", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseRunning, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
			}, false, false, core.NodeExecution_RUNNING, 0},

			{"running->retryableFailure", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseFailing, executors.NodePhasePending,
				func() (handler.Transition, error) {
					return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRetryableFailure(core.ExecutionError_USER, "x", "y", nil)), nil
				},
				false, true, core.NodeExecution_FAILED, 0},

			{"retryablefailure->running", v1alpha1.NodePhaseRetryableFailure, v1alpha1.NodePhaseRunning, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("should not be invoked")
			}, false, false, core.NodeExecution_RUNNING, 1},

			{"running->failing", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseFailing, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_USER, "code", "reason", nil)), nil
			}, false, true, core.NodeExecution_FAILED, 0},

			{"running->succeeding", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseSucceeding, executors.NodePhasePending, func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
			}, false, true, core.NodeExecution_SUCCEEDED, 0},

			{"running->error", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseRunning, executors.NodePhaseUndefined, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, true, false, core.NodeExecution_RUNNING, 0},

			{"previously-failed", v1alpha1.NodePhaseFailed, v1alpha1.NodePhaseFailed, executors.NodePhaseFailed, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, false, false, core.NodeExecution_RUNNING, 0},

			{"previously-success", v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseSucceeded, executors.NodePhaseComplete, func() (handler.Transition, error) {
				return handler.UnknownTransition, fmt.Errorf("error")
			}, false, false, core.NodeExecution_RUNNING, 0},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				store := createInmemoryDataStore(t, promutils.NewTestScope())
				adminClient := launchplan.NewFailFastLaunchPlanExecutor()
				execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
					adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
				assert.NoError(t, err)
				exec := execIface.(*nodeExecutor)
				exec.nodeHandlerFactory = hf

				called := false
				exec.nodeRecorder = &events.MockRecorder{
					RecordNodeEventCb: func(ctx context.Context, ev *event.NodeExecutionEvent) error {
						assert.NotNil(t, ev)
						assert.Equal(t, test.eventPhase.String(), ev.Phase.String())
						called = true
						return nil
					},
				}

				h := &nodeHandlerMocks.Node{}
				h.On("Handle",
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(test.handlerReturn())
				h.On("FinalizeRequired").Return(true)
				if test.currentNodePhase == v1alpha1.NodePhaseRetryableFailure {
					h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)
					h.On("Finalize", mock.Anything, mock.Anything).Return(nil)
				} else {
					h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
					h.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
				}
				hf.On("GetHandler", v1alpha1.NodeKindTask).Return(h, nil)

				mockWf, _, mockNodeStatus := createSingleNodeWf(test.currentNodePhase, 1)
				execErr := mockNodeStatus.GetExecutionError()
				startNode := mockWf.StartNode()
				execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String(), "expected %s, received %s", test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String())
				if test.expectedNodePhase == v1alpha1.NodePhaseFailing {
					assert.NotNil(t, mockNodeStatus.GetExecutionError())
				} else if test.expectedNodePhase == v1alpha1.NodePhaseFailed {
					assert.NotNil(t, s.Err)
					if test.currentNodePhase == v1alpha1.NodePhaseFailing {
						assert.Equal(t, execErr, s.Err)
					}
				} else {
					assert.Nil(t, s.Err)
				}
				assert.Equal(t, uint32(test.attempts), mockNodeStatus.GetAttempts())
				assert.Equal(t, test.eventRecorded, called, "event recording expected: %v, but got %v", test.eventRecorded, called)
			})
		}
	})

	// Extinguished retries
	t.Run("retries-exhausted", func(t *testing.T) {
		hf := &mocks2.HandlerFactory{}
		store := createInmemoryDataStore(t, promutils.NewTestScope())
		adminClient := launchplan.NewFailFastLaunchPlanExecutor()
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)
		exec.nodeHandlerFactory = hf

		h := &nodeHandlerMocks.Node{}
		h.On("Handle",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
		).Return(handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRetryableFailure(core.ExecutionError_USER, "x", "y", nil)), nil)
		h.On("FinalizeRequired").Return(true)
		h.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
		hf.On("GetHandler", v1alpha1.NodeKindTask).Return(h, nil)

		mockWf, _, mockNodeStatus := createSingleNodeWf(v1alpha1.NodePhaseRunning, 0)
		startNode := mockWf.StartNode()
		execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)

		s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
		assert.NoError(t, err)
		assert.Equal(t, executors.NodePhasePending.String(), s.NodePhase.String())
		assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
		assert.Equal(t, v1alpha1.NodePhaseFailing.String(), mockNodeStatus.GetPhase().String())
	})

	// Remaining retries
	t.Run("retries-remaining", func(t *testing.T) {
		hf := &mocks2.HandlerFactory{}
		store := createInmemoryDataStore(t, promutils.NewTestScope())
		adminClient := launchplan.NewFailFastLaunchPlanExecutor()
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)
		exec.nodeHandlerFactory = hf

		h := &nodeHandlerMocks.Node{}
		h.On("Handle",
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
		).Return(handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRetryableFailure(core.ExecutionError_USER, "x", "y", nil)), nil)
		h.On("FinalizeRequired").Return(true)
		h.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
		hf.On("GetHandler", v1alpha1.NodeKindTask).Return(h, nil)

		mockWf, _, mockNodeStatus := createSingleNodeWf(v1alpha1.NodePhaseRunning, 1)
		startNode := mockWf.StartNode()
		execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
		s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
		assert.NoError(t, err)
		assert.Equal(t, executors.NodePhasePending.String(), s.NodePhase.String())
		assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
		assert.Equal(t, v1alpha1.NodePhaseFailing.String(), mockNodeStatus.GetPhase().String())
	})

	// not fail immediately for last retry failure when enabled cleanupLastRetry
	t.Run("not-fail-immediately-for-last-failure", func(t *testing.T) {
		hf := &mocks2.HandlerFactory{}
		store := createInmemoryDataStore(t, promutils.NewTestScope())
		adminClient := launchplan.NewFailFastLaunchPlanExecutor()
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)
		exec.cleanupLastRetry = true
		exec.nodeHandlerFactory = hf

		h := &nodeHandlerMocks.Node{}
		h.OnHandleMatch(
			mock.MatchedBy(func(ctx context.Context) bool { return true }),
			mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
		).Return(handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRetryableFailure(core.ExecutionError_USER, "x", "y", nil)), nil)
		h.OnFinalizeRequiredMatch().Return(true)
		h.OnFinalizeMatch(mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
		hf.OnGetHandlerMatch(v1alpha1.NodeKindTask).Return(h, nil)

		mockWf, _, mockNodeStatus := createSingleNodeWf(v1alpha1.NodePhaseRunning, 1)
		startNode := mockWf.StartNode()
		execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
		s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
		assert.NoError(t, err)
		assert.Equal(t, executors.NodePhasePending.String(), s.NodePhase.String())
		assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
		assert.Equal(t, v1alpha1.NodePhaseRetryableFailure.String(), mockNodeStatus.GetPhase().String())
	})

	// Clean up last retry
	t.Run("retries-last-cleanup", func(t *testing.T) {
		hf := &mocks2.HandlerFactory{}
		store := createInmemoryDataStore(t, promutils.NewTestScope())
		adminClient := launchplan.NewFailFastLaunchPlanExecutor()
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)
		exec.cleanupLastRetry = true
		exec.nodeHandlerFactory = hf

		h := &nodeHandlerMocks.Node{}
		h.OnFinalizeRequiredMatch().Return(true)
		h.OnFinalizeMatch(mock.Anything, mock.Anything).Return(nil)
		h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		hf.OnGetHandlerMatch(v1alpha1.NodeKindTask).Return(h, nil)

		mockWf, _, mockNodeStatus := createSingleNodeWf(v1alpha1.NodePhaseRetryableFailure, 0)
		startNode := mockWf.StartNode()
		execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
		s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
		assert.NoError(t, err)
		assert.Equal(t, executors.NodePhaseFailed.String(), s.NodePhase.String())
		assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
		assert.Equal(t, v1alpha1.NodePhaseRetryableFailure.String(), mockNodeStatus.GetPhase().String())
	})

	// Abort error when clean up last retry
	t.Run("abort-error-retries-last-cleanup", func(t *testing.T) {
		hf := &mocks2.HandlerFactory{}
		store := createInmemoryDataStore(t, promutils.NewTestScope())
		adminClient := launchplan.NewFailFastLaunchPlanExecutor()
		execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
			adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
		assert.NoError(t, err)
		exec := execIface.(*nodeExecutor)
		exec.cleanupLastRetry = true
		exec.nodeHandlerFactory = hf

		h := &nodeHandlerMocks.Node{}
		h.OnFinalizeRequiredMatch().Return(true)
		h.OnFinalizeMatch(mock.Anything, mock.Anything).Return(nil)
		h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("error"))
		hf.OnGetHandlerMatch(v1alpha1.NodeKindTask).Return(h, nil)

		mockWf, _, mockNodeStatus := createSingleNodeWf(v1alpha1.NodePhaseRetryableFailure, 0)
		startNode := mockWf.StartNode()
		execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
		s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, startNode)
		assert.Error(t, err)
		assert.Equal(t, executors.NodePhaseUndefined.String(), s.NodePhase.String())
		assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
		assert.Equal(t, v1alpha1.NodePhaseRetryableFailure.String(), mockNodeStatus.GetPhase().String())
	})
}

func TestNodeExecutor_RecursiveNodeHandler_NoDownstream(t *testing.T) {
	ctx := context.Background()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}
	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)

	store := createInmemoryDataStore(t, promutils.NewTestScope())
	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient,
		adminClient, 10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	exec := execIface.(*nodeExecutor)

	defaultNodeID := "n1"
	taskID := "tID"

	createSingleNodeWf := func(p v1alpha1.NodePhase, maxAttempts int) (v1alpha1.ExecutableWorkflow, v1alpha1.ExecutableNode, v1alpha1.ExecutableNodeStatus) {
		n := &v1alpha1.NodeSpec{
			ID:      defaultNodeID,
			TaskRef: &taskID,
			Kind:    v1alpha1.NodeKindTask,
			RetryStrategy: &v1alpha1.RetryStrategy{
				MinAttempts: &maxAttempts,
			},
		}
		ns := &v1alpha1.NodeStatus{
			Phase: p,
		}

		startNode := &v1alpha1.NodeSpec{
			Kind: v1alpha1.NodeKindStart,
			ID:   v1alpha1.StartNodeID,
		}
		return &v1alpha1.FlyteWorkflow{
			Tasks: map[v1alpha1.TaskID]*v1alpha1.TaskSpec{
				taskID: {
					TaskTemplate: &core.TaskTemplate{},
				},
			},
			Status: v1alpha1.WorkflowStatus{
				NodeStatus: map[v1alpha1.NodeID]*v1alpha1.NodeStatus{
					defaultNodeID: ns,
					v1alpha1.StartNodeID: {
						Phase: v1alpha1.NodePhaseSucceeded,
					},
				},
				DataDir: "data",
			},
			WorkflowSpec: &v1alpha1.WorkflowSpec{
				ID: "wf",
				Nodes: map[v1alpha1.NodeID]*v1alpha1.NodeSpec{
					defaultNodeID:        n,
					v1alpha1.StartNodeID: startNode,
				},
				Connections: v1alpha1.Connections{
					UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						defaultNodeID: {v1alpha1.StartNodeID},
					},
					DownstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						v1alpha1.StartNodeID: {defaultNodeID},
					},
				},
			},
			DataReferenceConstructor: store,
		}, n, ns

	}

	// Node failed or succeeded
	{
		tests := []struct {
			name              string
			currentNodePhase  v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			expectedError     bool
		}{
			{"succeeded", v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseSucceeded, executors.NodePhaseComplete, false},
			{"failed", v1alpha1.NodePhaseFailed, v1alpha1.NodePhaseFailed, executors.NodePhaseFailed, false},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				exec.nodeHandlerFactory = hf

				h := &nodeHandlerMocks.Node{}
				h.On("Handle",
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(handler.UnknownTransition, fmt.Errorf("should not be called"))
				h.On("FinalizeRequired").Return(true)
				h.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))

				hf.On("GetHandler", v1alpha1.NodeKindTask).Return(h, nil)

				mockWf, mockNode, mockNodeStatus := createSingleNodeWf(test.currentNodePhase, 1)
				execContext := executors.NewExecutionContext(mockWf, nil, nil, nil)
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, mockNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, test.expectedNodePhase, mockNodeStatus.GetPhase(), "expected %s, received %s", test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String())
			})
		}
	}
}

func TestNodeExecutor_RecursiveNodeHandler_UpstreamNotReady(t *testing.T) {
	ctx := context.Background()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}
	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)

	store := createInmemoryDataStore(t, promutils.NewTestScope())

	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient, adminClient,
		10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	exec := execIface.(*nodeExecutor)

	defaultNodeID := "n1"
	taskID := taskID

	createSingleNodeWf := func(parentPhase v1alpha1.NodePhase, maxAttempts int) (v1alpha1.ExecutableWorkflow, v1alpha1.ExecutableNode, v1alpha1.ExecutableNodeStatus) {
		n := &v1alpha1.NodeSpec{
			ID:      defaultNodeID,
			TaskRef: &taskID,
			Kind:    v1alpha1.NodeKindTask,
			RetryStrategy: &v1alpha1.RetryStrategy{
				MinAttempts: &maxAttempts,
			},
		}
		ns := &v1alpha1.NodeStatus{}

		return &v1alpha1.FlyteWorkflow{
			Tasks: map[v1alpha1.TaskID]*v1alpha1.TaskSpec{
				taskID: {
					TaskTemplate: &core.TaskTemplate{},
				},
			},
			Status: v1alpha1.WorkflowStatus{
				NodeStatus: map[v1alpha1.NodeID]*v1alpha1.NodeStatus{
					defaultNodeID: ns,
					v1alpha1.StartNodeID: {
						Phase: parentPhase,
					},
				},
				DataDir: "data",
			},
			WorkflowSpec: &v1alpha1.WorkflowSpec{
				ID: "wf",
				Nodes: map[v1alpha1.NodeID]*v1alpha1.NodeSpec{
					defaultNodeID: n,
				},
				Connections: v1alpha1.Connections{
					UpstreamEdges: map[v1alpha1.NodeID][]v1alpha1.NodeID{
						defaultNodeID: {v1alpha1.StartNodeID},
					},
				},
			},
			DataReferenceConstructor: store,
			RawOutputDataConfig: v1alpha1.RawOutputDataConfig{
				RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
			},
		}, n, ns

	}

	// Node not yet started
	{
		tests := []struct {
			name              string
			parentNodePhase   v1alpha1.NodePhase
			expectedNodePhase v1alpha1.NodePhase
			expectedPhase     executors.NodePhase
			expectedError     bool
		}{
			{"notYetStarted", v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"running", v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"queued", v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"retryable", v1alpha1.NodePhaseRetryableFailure, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"failing", v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"failing", v1alpha1.NodePhaseSucceeding, v1alpha1.NodePhaseNotYetStarted, executors.NodePhasePending, false},
			{"skipped", v1alpha1.NodePhaseSkipped, v1alpha1.NodePhaseSkipped, executors.NodePhaseSuccess, false},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				exec.nodeHandlerFactory = hf
				h := &nodeHandlerMocks.Node{}
				h.On("Handle",
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(handler.UnknownTransition, fmt.Errorf("should not be called"))
				h.On("FinalizeRequired").Return(true)
				h.On("Finalize", mock.Anything, mock.Anything).Return(fmt.Errorf("error"))

				hf.On("GetHandler", v1alpha1.NodeKindTask).Return(h, nil)

				mockWf, mockNode, mockNodeStatus := createSingleNodeWf(test.parentNodePhase, 0)
				execContext := executors.NewExecutionContext(mockWf, mockWf, nil, nil)
				s, err := exec.RecursiveNodeHandler(ctx, execContext, mockWf, mockWf, mockNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
				assert.Equal(t, uint32(0), mockNodeStatus.GetAttempts())
				assert.Equal(t, test.expectedNodePhase, mockNodeStatus.GetPhase(), "expected %s, received %s", test.expectedNodePhase.String(), mockNodeStatus.GetPhase().String())
			})
		}
	}
}

func TestNodeExecutor_RecursiveNodeHandler_BranchNode(t *testing.T) {
	ctx := context.TODO()
	enQWf := func(workflowID v1alpha1.WorkflowID) {
	}
	mockEventSink := events.NewMockEventSink().(*events.MockEventSink)

	store := createInmemoryDataStore(t, promutils.NewTestScope())

	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	execIface, err := NewExecutor(ctx, config.GetConfig().NodeConfig, store, enQWf, mockEventSink, adminClient, adminClient,
		10, "s3://bucket", fakeKubeClient, catalogClient, promutils.NewTestScope())
	assert.NoError(t, err)
	exec := execIface.(*nodeExecutor)
	// Node not yet started
	{
		tests := []struct {
			name                string
			parentNodePhase     v1alpha1.BranchNodePhase
			currentNodePhase    v1alpha1.NodePhase
			phaseUpdateExpected bool
			expectedPhase       executors.NodePhase
			expectedError       bool
		}{
			{"branchSuccess", v1alpha1.BranchNodeSuccess, v1alpha1.NodePhaseNotYetStarted, true, executors.NodePhaseQueued, false},
			{"branchNotYetDone", v1alpha1.BranchNodeNotYetEvaluated, v1alpha1.NodePhaseNotYetStarted, false, executors.NodePhaseUndefined, true},
			{"branchError", v1alpha1.BranchNodeError, v1alpha1.NodePhaseNotYetStarted, false, executors.NodePhaseUndefined, true},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				hf := &mocks2.HandlerFactory{}
				exec.nodeHandlerFactory = hf
				h := &nodeHandlerMocks.Node{}
				h.OnHandleMatch(
					mock.MatchedBy(func(ctx context.Context) bool { return true }),
					mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
				).Return(handler.UnknownTransition, fmt.Errorf("should not be called"))
				h.OnFinalizeRequired().Return(true)
				h.OnFinalizeMatch(mock.Anything, mock.Anything).Return(fmt.Errorf("error"))

				hf.OnGetHandlerMatch(v1alpha1.NodeKindTask).Return(h, nil)

				parentBranchNodeID := "branchNode"
				parentBranchNode := &mocks.ExecutableNode{}
				parentBranchNode.OnGetID().Return(parentBranchNodeID)
				parentBranchNode.OnGetBranchNode().Return(&mocks.ExecutableBranchNode{})
				parentBranchNodeStatus := &mocks.ExecutableNodeStatus{}
				parentBranchNodeStatus.OnGetPhase().Return(v1alpha1.NodePhaseRunning)
				parentBranchNodeStatus.OnIsDirty().Return(false)
				bns := &mocks.MutableBranchNodeStatus{}
				parentBranchNodeStatus.OnGetBranchStatus().Return(bns)
				bns.OnGetPhase().Return(test.parentNodePhase)

				tk := &mocks.ExecutableTask{}
				tk.OnCoreTask().Return(&core.TaskTemplate{})

				tid := "tid"
				eCtx := &mocks4.ExecutionContext{}
				eCtx.OnGetTask(tid).Return(tk, nil)
				eCtx.OnIsInterruptible().Return(true)
				eCtx.OnGetExecutionID().Return(v1alpha1.WorkflowExecutionIdentifier{WorkflowExecutionIdentifier: &core.WorkflowExecutionIdentifier{}})
				eCtx.OnGetLabels().Return(nil)
				eCtx.OnGetEventVersion().Return(v1alpha1.EventVersion0)
				eCtx.OnGetParentInfo().Return(nil)
				eCtx.OnGetRawOutputDataConfig().Return(v1alpha1.RawOutputDataConfig{
					RawOutputDataConfig: &admin.RawOutputDataConfig{OutputLocationPrefix: ""},
				})

				branchTakenNodeID := "branchTakenNode"
				branchTakenNode := &mocks.ExecutableNode{}
				branchTakenNode.OnGetID().Return(branchTakenNodeID)
				branchTakenNode.OnGetKind().Return(v1alpha1.NodeKindTask)
				branchTakenNode.OnGetTaskID().Return(&tid)
				branchTakenNode.OnIsInterruptible().Return(nil)
				branchTakenNode.OnIsStartNode().Return(false)
				branchTakenNode.OnIsEndNode().Return(false)
				branchTakenNode.OnGetInputBindings().Return(nil)
				branchTakeNodeStatus := &mocks.ExecutableNodeStatus{}
				branchTakeNodeStatus.OnGetPhase().Return(test.currentNodePhase)
				branchTakeNodeStatus.OnIsDirty().Return(false)
				branchTakeNodeStatus.OnGetSystemFailures().Return(1)
				branchTakeNodeStatus.OnGetDataDir().Return("data")
				branchTakeNodeStatus.OnGetParentNodeID().Return(&parentBranchNodeID)
				branchTakeNodeStatus.OnGetParentTaskID().Return(nil)

				if test.phaseUpdateExpected {
					var ee *core.ExecutionError
					branchTakeNodeStatus.On("UpdatePhase", v1alpha1.NodePhaseQueued, mock.Anything, mock.Anything, ee).Return()
				}

				leafDag := executors.NewLeafNodeDAGStructure(branchTakenNodeID, parentBranchNodeID)

				nl := executors.NewTestNodeLookup(
					map[v1alpha1.NodeID]v1alpha1.ExecutableNode{branchTakenNodeID: branchTakenNode, parentBranchNodeID: parentBranchNode},
					map[v1alpha1.NodeID]v1alpha1.ExecutableNodeStatus{branchTakenNodeID: branchTakeNodeStatus, parentBranchNodeID: parentBranchNodeStatus},
				)

				s, err := exec.RecursiveNodeHandler(ctx, eCtx, leafDag, nl, branchTakenNode)
				if test.expectedError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
				assert.Equal(t, test.expectedPhase, s.NodePhase, "expected: %s, received %s", test.expectedPhase.String(), s.NodePhase.String())
			})
		}
	}
}

func Test_nodeExecutor_RecordTransitionLatency(t *testing.T) {
	testScope := promutils.NewTestScope()
	type fields struct {
		nodeHandlerFactory HandlerFactory
		enqueueWorkflow    v1alpha1.EnqueueWorkflow
		store              *storage.DataStore
		nodeRecorder       events.NodeEventRecorder
		metrics            *nodeMetrics
	}
	type args struct {
		w          v1alpha1.ExecutableWorkflow
		node       v1alpha1.ExecutableNode
		nodeStatus v1alpha1.ExecutableNodeStatus
	}

	nsf := func(phase v1alpha1.NodePhase, lastUpdated *time.Time) *mocks.ExecutableNodeStatus {
		ns := &mocks.ExecutableNodeStatus{}
		ns.On("GetPhase").Return(phase)
		var t *v1.Time
		if lastUpdated != nil {
			t = &v1.Time{Time: *lastUpdated}
		}
		ns.On("GetLastUpdatedAt").Return(t)
		return ns
	}
	testTime := time.Now()
	tests := []struct {
		name              string
		fields            fields
		args              args
		recordingExpected bool
	}{
		{
			"retryable-failure",
			fields{metrics: &nodeMetrics{TransitionLatency: labeled.NewStopWatch("test", "xyz", time.Millisecond, testScope)}},
			args{nodeStatus: nsf(v1alpha1.NodePhaseRetryableFailure, &testTime)},
			true,
		},
		{
			"retryable-failure-notime",
			fields{metrics: &nodeMetrics{TransitionLatency: labeled.NewStopWatch("test2", "xyz", time.Millisecond, testScope)}},
			args{nodeStatus: nsf(v1alpha1.NodePhaseRetryableFailure, nil)},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &nodeExecutor{
				nodeHandlerFactory: tt.fields.nodeHandlerFactory,
				enqueueWorkflow:    tt.fields.enqueueWorkflow,
				store:              tt.fields.store,
				nodeRecorder:       tt.fields.nodeRecorder,
				metrics:            tt.fields.metrics,
			}
			c.RecordTransitionLatency(context.TODO(), tt.args.w, tt.args.w, tt.args.node, tt.args.nodeStatus)

			ch := make(chan prometheus.Metric, 2)
			tt.fields.metrics.TransitionLatency.Collect(ch)
			assert.Equal(t, len(ch) == 1, tt.recordingExpected)
		})
	}
}

func Test_nodeExecutor_timeout(t *testing.T) {
	tests := []struct {
		name              string
		phaseInfo         handler.PhaseInfo
		expectedPhase     handler.EPhase
		activeDeadline    time.Duration
		executionDeadline time.Duration
		retries           int
		err               error
		expectedReason    string
	}{
		{
			name:              "timeout",
			phaseInfo:         handler.PhaseInfoRunning(nil),
			expectedPhase:     handler.EPhaseTimedout,
			activeDeadline:    time.Second * 5,
			executionDeadline: time.Second * 5,
			err:               nil,
		},
		{
			name:              "default_execution_timeout",
			phaseInfo:         handler.PhaseInfoRunning(nil),
			expectedPhase:     handler.EPhaseRetryableFailure,
			activeDeadline:    time.Second * 50,
			executionDeadline: 0,
			retries:           2,
			err:               nil,
			expectedReason:    "task execution timeout [1s] expired",
		},
		{
			name:              "retryable-failure",
			phaseInfo:         handler.PhaseInfoRunning(nil),
			expectedPhase:     handler.EPhaseRetryableFailure,
			activeDeadline:    time.Second * 15,
			executionDeadline: time.Second * 5,
			retries:           2,
			err:               nil,
		},
		{
			name:              "retries-exhausted",
			phaseInfo:         handler.PhaseInfoRunning(nil),
			expectedPhase:     handler.EPhaseFailed,
			activeDeadline:    time.Second * 15,
			executionDeadline: time.Second * 5,
			retries:           1,
			err:               nil,
		},
		{
			name:              "expired-but-terminal-phase",
			phaseInfo:         handler.PhaseInfoSuccess(nil),
			expectedPhase:     handler.EPhaseSuccess,
			activeDeadline:    time.Second * 10,
			executionDeadline: time.Second * 5,
			err:               nil,
		},
		{
			name:              "not-expired",
			phaseInfo:         handler.PhaseInfoRunning(nil),
			expectedPhase:     handler.EPhaseRunning,
			activeDeadline:    time.Second * 15,
			executionDeadline: time.Second * 15,
			err:               nil,
		},
		{
			name:              "handler-failure",
			phaseInfo:         handler.PhaseInfoRunning(nil),
			expectedPhase:     handler.EPhaseUndefined,
			activeDeadline:    time.Second * 15,
			executionDeadline: time.Second * 15,
			err:               errors.New("test-error"),
		},
	}
	// mocking status
	queuedAt := time.Now().Add(-1 * time.Second * 10)
	ns := &mocks.ExecutableNodeStatus{}
	queuedAtTime := &v1.Time{Time: queuedAt}
	ns.On("GetQueuedAt").Return(queuedAtTime)
	ns.On("GetLastAttemptStartedAt").Return(queuedAtTime)
	ns.OnGetAttempts().Return(0)
	ns.OnGetSystemFailures().Return(0)
	ns.On("ClearLastAttemptStartedAt").Return()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &nodeExecutor{defaultActiveDeadline: time.Second, defaultExecutionDeadline: time.Second}
			handlerReturn := func() (handler.Transition, error) {
				return handler.DoTransition(handler.TransitionTypeEphemeral, tt.phaseInfo), tt.err
			}
			h := &nodeHandlerMocks.Node{}
			h.On("Handle",
				mock.MatchedBy(func(ctx context.Context) bool { return true }),
				mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
			).Return(handlerReturn())
			h.On("FinalizeRequired").Return(true)
			h.On("Finalize", mock.Anything, mock.Anything).Return(nil)

			hf := &mocks2.HandlerFactory{}
			hf.On("GetHandler", v1alpha1.NodeKindStart).Return(h, nil)
			c.nodeHandlerFactory = hf

			mockNode := &mocks.ExecutableNode{}
			mockNode.On("GetID").Return("node")
			mockNode.On("GetBranchNode").Return(nil)
			mockNode.On("GetKind").Return(v1alpha1.NodeKindTask)
			mockNode.On("IsStartNode").Return(false)
			mockNode.On("IsEndNode").Return(false)
			mockNode.On("GetInputBindings").Return([]*v1alpha1.Binding{})
			mockNode.On("GetActiveDeadline").Return(&tt.activeDeadline)
			mockNode.On("GetExecutionDeadline").Return(&tt.executionDeadline)
			mockNode.OnGetRetryStrategy().Return(&v1alpha1.RetryStrategy{MinAttempts: &tt.retries})

			nCtx := &nodeExecContext{node: mockNode, nsm: &nodeStateManager{nodeStatus: ns}}
			phaseInfo, err := c.execute(context.TODO(), h, nCtx, ns)

			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expectedPhase.String(), phaseInfo.GetPhase().String())
			if tt.expectedReason != "" {
				assert.Equal(t, tt.expectedReason, phaseInfo.GetReason())
			}
		})
	}
}

func Test_nodeExecutor_system_error(t *testing.T) {
	phaseInfo := handler.PhaseInfoRetryableFailureErr(&core.ExecutionError{Code: "Interrupted", Message: "test", Kind: core.ExecutionError_SYSTEM}, nil)

	// mocking status
	ns := &mocks.ExecutableNodeStatus{}
	ns.OnGetAttempts().Return(0)
	ns.OnGetSystemFailures().Return(0)
	ns.On("GetQueuedAt").Return(&v1.Time{Time: time.Now()})
	ns.On("GetLastAttemptStartedAt").Return(&v1.Time{Time: time.Now()})

	ns.On("ClearLastAttemptStartedAt").Return()

	c := &nodeExecutor{}
	h := &nodeHandlerMocks.Node{}
	h.On("Handle",
		mock.MatchedBy(func(ctx context.Context) bool { return true }),
		mock.MatchedBy(func(o handler.NodeExecutionContext) bool { return true }),
	).Return(handler.DoTransition(handler.TransitionTypeEphemeral, phaseInfo), nil)

	h.On("FinalizeRequired").Return(true)
	h.On("Finalize", mock.Anything, mock.Anything).Return(nil)

	hf := &mocks2.HandlerFactory{}
	hf.On("GetHandler", v1alpha1.NodeKindStart).Return(h, nil)
	c.nodeHandlerFactory = hf
	c.maxNodeRetriesForSystemFailures = 2

	mockNode := &mocks.ExecutableNode{}
	mockNode.On("GetID").Return("node")
	mockNode.On("GetActiveDeadline").Return(nil)
	mockNode.On("GetExecutionDeadline").Return(nil)
	retries := 2
	mockNode.OnGetRetryStrategy().Return(&v1alpha1.RetryStrategy{MinAttempts: &retries})

	nCtx := &nodeExecContext{node: mockNode, nsm: &nodeStateManager{nodeStatus: ns}}
	phaseInfo, err := c.execute(context.TODO(), h, nCtx, ns)
	assert.Equal(t, handler.EPhaseRetryableFailure, phaseInfo.GetPhase())
	assert.NoError(t, err)
	assert.Equal(t, core.ExecutionError_SYSTEM, phaseInfo.GetErr().GetKind())
}

func Test_nodeExecutor_abort(t *testing.T) {
	ctx := context.Background()
	exec := nodeExecutor{}
	nCtx := &nodeExecContext{}

	t.Run("abort error calls finalize", func(t *testing.T) {
		h := &nodeHandlerMocks.Node{}
		h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test error"))
		h.OnFinalizeRequired().Return(true)
		var called bool
		h.OnFinalizeMatch(mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			called = true
		}).Return(nil)

		err := exec.abort(ctx, h, nCtx, "testing")
		assert.Equal(t, "test error", err.Error())
		assert.True(t, called)
	})

	t.Run("abort error calls finalize with error", func(t *testing.T) {
		h := &nodeHandlerMocks.Node{}
		h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("test error"))
		h.OnFinalizeRequired().Return(true)
		var called bool
		h.OnFinalizeMatch(mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			called = true
		}).Return(errors.New("finalize error"))

		err := exec.abort(ctx, h, nCtx, "testing")
		assert.Equal(t, "0: test error\r\n1: finalize error\r\n", err.Error())
		assert.True(t, called)
	})

	t.Run("abort calls finalize when no errors", func(t *testing.T) {
		h := &nodeHandlerMocks.Node{}
		h.OnAbortMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil)
		h.OnFinalizeRequired().Return(true)
		var called bool
		h.OnFinalizeMatch(mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			called = true
		}).Return(nil)

		err := exec.abort(ctx, h, nCtx, "testing")
		assert.NoError(t, err)
		assert.True(t, called)
	})
}

func TestNodeExecutor_AbortHandler(t *testing.T) {
	ctx := context.Background()
	exec := nodeExecutor{}

	t.Run("not-yet-started", func(t *testing.T) {
		id := "id"
		n := &mocks.ExecutableNode{}
		n.OnGetID().Return(id)
		nl := &mocks4.NodeLookup{}
		ns := &mocks.ExecutableNodeStatus{}
		ns.OnGetPhase().Return(v1alpha1.NodePhaseNotYetStarted)
		nl.OnGetNodeExecutionStatusMatch(mock.Anything, id).Return(ns)
		assert.NoError(t, exec.AbortHandler(ctx, nil, nil, nl, n, "aborting"))
	})
}

func TestNodeExecutor_FinalizeHandler(t *testing.T) {
	ctx := context.Background()
	exec := nodeExecutor{}

	t.Run("not-yet-started", func(t *testing.T) {
		id := "id"
		n := &mocks.ExecutableNode{}
		n.OnGetID().Return(id)
		nl := &mocks4.NodeLookup{}
		ns := &mocks.ExecutableNodeStatus{}
		ns.OnGetPhase().Return(v1alpha1.NodePhaseNotYetStarted)
		nl.OnGetNodeExecutionStatusMatch(mock.Anything, id).Return(ns)
		assert.NoError(t, exec.FinalizeHandler(ctx, nil, nil, nl, n))
	})
}

func TestNodeExecutionEventV0(t *testing.T) {
	execID := &core.WorkflowExecutionIdentifier{
		Name:    "e1",
		Domain:  "d1",
		Project: "p1",
	}
	nID := &core.NodeExecutionIdentifier{
		NodeId:      "n1",
		ExecutionId: execID,
	}
	tID := &core.TaskExecutionIdentifier{
		NodeExecutionId: nID,
	}
	p := handler.PhaseInfoQueued("r")
	inputReader := &mocks3.InputReader{}
	inputReader.OnGetInputPath().Return("reference")
	parentInfo := &mocks4.ImmutableParentInfo{}
	parentInfo.OnGetUniqueID().Return("np1")
	parentInfo.OnCurrentAttempt().Return(uint32(2))

	id := "id"
	n := &mocks.ExecutableNode{}
	n.OnGetID().Return(id)
	n.OnGetName().Return("name")
	nl := &mocks4.NodeLookup{}
	ns := &mocks.ExecutableNodeStatus{}
	ns.OnGetPhase().Return(v1alpha1.NodePhaseNotYetStarted)
	nl.OnGetNodeExecutionStatusMatch(mock.Anything, id).Return(ns)
	ns.OnGetParentTaskID().Return(tID)
	event, err := ToNodeExecutionEvent(nID, p, inputReader, ns, v1alpha1.EventVersion0, parentInfo, n)
	assert.NoError(t, err)
	assert.Equal(t, "n1", event.Id.NodeId)
	assert.Equal(t, execID, event.Id.ExecutionId)
	assert.Empty(t, event.SpecNodeId)
	assert.Nil(t, event.ParentNodeMetadata)
	assert.Equal(t, tID, event.ParentTaskMetadata.Id)
	assert.Empty(t, event.NodeName)
	assert.Empty(t, event.RetryGroup)
}

func TestNodeExecutionEventV1(t *testing.T) {
	execID := &core.WorkflowExecutionIdentifier{
		Name:    "e1",
		Domain:  "d1",
		Project: "p1",
	}
	nID := &core.NodeExecutionIdentifier{
		NodeId:      "n1",
		ExecutionId: execID,
	}
	tID := &core.TaskExecutionIdentifier{
		NodeExecutionId: nID,
	}
	p := handler.PhaseInfoQueued("r")
	inputReader := &mocks3.InputReader{}
	inputReader.OnGetInputPath().Return("reference")
	parentInfo := &mocks4.ImmutableParentInfo{}
	parentInfo.OnGetUniqueID().Return("np1")
	parentInfo.OnCurrentAttempt().Return(uint32(2))

	id := "id"
	n := &mocks.ExecutableNode{}
	n.OnGetID().Return(id)
	n.OnGetName().Return("name")
	nl := &mocks4.NodeLookup{}
	ns := &mocks.ExecutableNodeStatus{}
	ns.OnGetPhase().Return(v1alpha1.NodePhaseNotYetStarted)
	nl.OnGetNodeExecutionStatusMatch(mock.Anything, id).Return(ns)
	ns.OnGetParentTaskID().Return(tID)
	eventOpt, err := ToNodeExecutionEvent(nID, p, inputReader, ns, v1alpha1.EventVersion1, parentInfo, n)
	assert.NoError(t, err)
	assert.Equal(t, "np1-2-n1", eventOpt.Id.NodeId)
	assert.Equal(t, execID, eventOpt.Id.ExecutionId)
	assert.Equal(t, "id", eventOpt.SpecNodeId)
	expectParentMetadata := event.ParentNodeExecutionMetadata{
		NodeId: "np1",
	}
	assert.Equal(t, expectParentMetadata, *eventOpt.ParentNodeMetadata)
	assert.Nil(t, eventOpt.ParentTaskMetadata)
	assert.Equal(t, "name", eventOpt.NodeName)
	assert.Equal(t, "2", eventOpt.RetryGroup)
}
