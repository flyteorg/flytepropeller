package array

import (
	"context"
	"fmt"
	"testing"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/event"

	eventmocks "github.com/flyteorg/flytepropeller/events/mocks"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	execmocks "github.com/flyteorg/flytepropeller/pkg/controller/executors/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes"
	gatemocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/gate/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces/mocks"
	recoverymocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/recovery/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/catalog"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	pluginmocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io/mocks"

	"github.com/flyteorg/flytestdlib/bitarray"
	"github.com/flyteorg/flytestdlib/contextutils"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/promutils/labeled"
	"github.com/flyteorg/flytestdlib/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	taskRef = "taskRef"
	arrayNodeSpec = v1alpha1.NodeSpec{
		ID: "foo",
		ArrayNode: &v1alpha1.ArrayNodeSpec{
			SubNodeSpec: &v1alpha1.NodeSpec{
				TaskRef: &taskRef,
			},
		},
	}
)

func createArrayNodeHandler(t *testing.T, ctx context.Context, nodeHandler interfaces.NodeHandler, dataStore *storage.DataStore, scope promutils.Scope) (interfaces.NodeHandler, error) {
	// mock components
	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	enqueueWorkflowFunc := func(workflowID v1alpha1.WorkflowID) {}
	eventConfig := &config.EventConfig{}
	mockEventSink := eventmocks.NewMockEventSink()
	mockHandlerFactory := &mocks.HandlerFactory{}
	mockHandlerFactory.OnGetHandlerMatch(mock.Anything).Return(nodeHandler, nil)
	mockKubeClient := execmocks.NewFakeKubeClient()
	mockRecoveryClient := &recoverymocks.Client{}
	mockSignalClient := &gatemocks.SignalServiceClient{}
	noopCatalogClient := catalog.NOOPCatalog{}

	// create node executor
	nodeExecutor, err := nodes.NewExecutor(ctx, config.GetConfig().NodeConfig, dataStore, enqueueWorkflowFunc, mockEventSink, adminClient,
		adminClient, 10, "s3://bucket/", mockKubeClient, noopCatalogClient, mockRecoveryClient, eventConfig, "clusterID", mockSignalClient, mockHandlerFactory, scope)
	assert.NoError(t, err)

	// return ArrayNodeHandler
	return New(nodeExecutor, eventConfig, scope)
}

func createNodeExecutionContext(t *testing.T, ctx context.Context, dataStore *storage.DataStore, eventRecorder interfaces.EventRecorder,
	inputLiteralMap *idlcore.LiteralMap, arrayNodeSpec *v1alpha1.NodeSpec, arrayNodeState *interfaces.ArrayNodeState) (interfaces.NodeExecutionContext, error) {

	nCtx := &mocks.NodeExecutionContext{}

	// ContextualNodeLookup
	nodeLookup := &execmocks.NodeLookup{}
	nCtx.OnContextualNodeLookup().Return(nodeLookup)

	// DataStore
	nCtx.OnDataStore().Return(dataStore)

	// ExecutionContext
	executionContext := &execmocks.ExecutionContext{}
	executionContext.OnGetEventVersion().Return(1)
	executionContext.OnGetExecutionConfig().Return(
		v1alpha1.ExecutionConfig{
		})
	executionContext.OnGetExecutionID().Return(
		v1alpha1.ExecutionID{
			&idlcore.WorkflowExecutionIdentifier{
				Project: "project",
				Domain:  "domain",
				Name:    "name",
			},
		})
	executionContext.OnGetLabels().Return(nil)
	executionContext.OnGetRawOutputDataConfig().Return(v1alpha1.RawOutputDataConfig{})
	executionContext.OnIsInterruptible().Return(false)
	executionContext.OnGetParentInfo().Return(nil)
	nCtx.OnExecutionContext().Return(executionContext)

	// EventsRecorder
	nCtx.OnEventsRecorder().Return(eventRecorder)

	// InputReader
	inputFilePaths := &pluginmocks.InputFilePaths{}
	inputFilePaths.OnGetInputPath().Return(storage.DataReference("s3://bucket/input"))
	nCtx.OnInputReader().Return(
		newStaticInputReader(
			inputFilePaths,
			inputLiteralMap,
		))

	// Node
	nCtx.OnNode().Return(arrayNodeSpec)

	// NodeExecutionMetadata
	nodeExecutionMetadata := &mocks.NodeExecutionMetadata{}
	nodeExecutionMetadata.OnGetNodeExecutionID().Return(&idlcore.NodeExecutionIdentifier{
		NodeId: "foo",
		ExecutionId: &idlcore.WorkflowExecutionIdentifier{
			Project: "project",
			Domain:  "domain",
			Name:    "name",
		},
	})
	nCtx.OnNodeExecutionMetadata().Return(nodeExecutionMetadata)

	// NodeID
	nCtx.OnNodeID().Return("foo")

	// NodeStateReader
	nodeStateReader := &mocks.NodeStateReader{}
	nodeStateReader.OnGetArrayNodeState().Return(*arrayNodeState)
	nCtx.OnNodeStateReader().Return(nodeStateReader)

	// NodeStateWriter
	nodeStateWriter := &mocks.NodeStateWriter{}
	nodeStateWriter.OnPutArrayNodeStateMatch(mock.Anything, mock.Anything).Run(
		func(args mock.Arguments) {
			*arrayNodeState = args.Get(0).(interfaces.ArrayNodeState)
		},
	).Return(nil)
	nCtx.OnNodeStateWriter().Return(nodeStateWriter)

	// NodeStatus
	nCtx.OnNodeStatus().Return(&v1alpha1.NodeStatus{
		DataDir: storage.DataReference("s3://bucket/foo"),
	})

	return nCtx, nil
}

func TestAbort(t *testing.T) {
	// TODO @hamersaw - complete
}

func TestFinalize(t *testing.T) {
	// TODO @hamersaw - complete
}

func TestHandleArrayNodePhaseNone(t *testing.T) {
	ctx := context.Background()
	scope := promutils.NewTestScope()
	dataStore, err := storage.NewDataStore(&storage.Config{
		Type: storage.TypeMemory,
	}, scope)
	assert.NoError(t, err)
	nodeHandler := &mocks.NodeHandler{}

	// initialize ArrayNodeHandler
	arrayNodeHandler, err := createArrayNodeHandler(t, ctx, nodeHandler, dataStore, scope)
	assert.NoError(t, err)

	tests := []struct {
		name                           string
		inputValues                    map[string][]int64
		expectedArrayNodePhase         v1alpha1.ArrayNodePhase
		expectedTransitionPhase        handler.EPhase
		expectedExternalResourcePhases []idlcore.TaskExecution_Phase
	}{
		{
			name: "Success",
			inputValues: map[string][]int64{
				"foo": []int64{1, 2},
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseExecuting,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_QUEUED, idlcore.TaskExecution_QUEUED},
		},
		{
			name: "SuccessMultipleInputs",
			inputValues: map[string][]int64{
				"foo": []int64{1, 2, 3},
				"bar": []int64{4, 5, 6},
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseExecuting,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_QUEUED, idlcore.TaskExecution_QUEUED, idlcore.TaskExecution_QUEUED},
		},
		{
			name: "FailureDifferentInputListLengths",
			inputValues: map[string][]int64{
				"foo": []int64{1, 2},
				"bar": []int64{3},
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseNone,
			expectedTransitionPhase: handler.EPhaseFailed,
			expectedExternalResourcePhases: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create NodeExecutionContext
			eventRecorder := newArrayEventRecorder()
			literalMap := convertMapToArrayLiterals(test.inputValues)
			arrayNodeState := &interfaces.ArrayNodeState{
				Phase: v1alpha1.ArrayNodePhaseNone,
			}
			nCtx, err := createNodeExecutionContext(t, ctx, dataStore, eventRecorder, literalMap, &arrayNodeSpec, arrayNodeState)
			assert.NoError(t, err)

			// evaluate node
			transition, err := arrayNodeHandler.Handle(ctx, nCtx)
			assert.NoError(t, err)

			// validate results
			assert.Equal(t, test.expectedArrayNodePhase, arrayNodeState.Phase)
			assert.Equal(t, test.expectedTransitionPhase, transition.Info().GetPhase())

			if len(test.expectedExternalResourcePhases) > 0 {
				assert.Equal(t, 1, len(eventRecorder.taskEvents))

				externalResources := eventRecorder.taskEvents[0].Metadata.GetExternalResources()
				assert.Equal(t, len(test.expectedExternalResourcePhases), len(externalResources))
				for i, expectedPhase := range test.expectedExternalResourcePhases {
					assert.Equal(t, expectedPhase, externalResources[i].Phase)
				}
			} else {
				assert.Equal(t, 0, len(eventRecorder.taskEvents))
			}
		})
	}
}

func TestHandleArrayNodePhaseExecuting(t *testing.T) {
	ctx := context.Background()
	minSuccessRatio := float32(0.5)

	// initailize universal variables
	inputMap := map[string][]int64{
		"foo": []int64{0, 1},
		"bar": []int64{2, 3},
	}
	literalMap := convertMapToArrayLiterals(inputMap)

	size := -1
	for _, v := range inputMap {
		if size == -1 {
			size = len(v)
		} else if len(v) > size { // calculating size as largest input list
			size = len(v)
		}
	}

	tests := []struct {
		name                           string
		parallelism                    int
		minSuccessRatio                *float32
		subNodePhases                  []v1alpha1.NodePhase
		subNodeTaskPhases              []core.Phase
		subNodeTransitions             []handler.Transition
		expectedArrayNodePhase         v1alpha1.ArrayNodePhase
		expectedTransitionPhase        handler.EPhase
		expectedExternalResourcePhases []idlcore.TaskExecution_Phase
	}{
		{
			name: "StartAllSubNodes",
			subNodePhases: []v1alpha1.NodePhase{
				v1alpha1.NodePhaseQueued,
				v1alpha1.NodePhaseQueued,
			},
			subNodeTaskPhases: []core.Phase{
				core.PhaseUndefined,
				core.PhaseUndefined,
			},
			subNodeTransitions: []handler.Transition{
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})),
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})),
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseExecuting,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_RUNNING, idlcore.TaskExecution_RUNNING},
		},
		{
			name: "StartOneSubNodeParallelism",
			parallelism: 1,
			subNodePhases: []v1alpha1.NodePhase{
				v1alpha1.NodePhaseQueued,
				v1alpha1.NodePhaseQueued,
			},
			subNodeTaskPhases: []core.Phase{
				core.PhaseUndefined,
				core.PhaseUndefined,
			},
			subNodeTransitions: []handler.Transition{
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})),
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})),
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseExecuting,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_RUNNING, idlcore.TaskExecution_QUEUED},
		},
		{
			name: "AllSubNodesSuccedeed",
			subNodePhases: []v1alpha1.NodePhase{
				v1alpha1.NodePhaseRunning,
				v1alpha1.NodePhaseRunning,
			},
			subNodeTaskPhases: []core.Phase{
				core.PhaseRunning,
				core.PhaseRunning,
			},
			subNodeTransitions: []handler.Transition{
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(&handler.ExecutionInfo{})),
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(&handler.ExecutionInfo{})),
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseSucceeding,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_SUCCEEDED, idlcore.TaskExecution_SUCCEEDED},
		},
		{
			name: "OneSubNodeSuccedeedMinSuccessRatio",
			minSuccessRatio: &minSuccessRatio,
			subNodePhases: []v1alpha1.NodePhase{
				v1alpha1.NodePhaseRunning,
				v1alpha1.NodePhaseRunning,
			},
			subNodeTaskPhases: []core.Phase{
				core.PhaseRunning,
				core.PhaseRunning,
			},
			subNodeTransitions: []handler.Transition{
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(&handler.ExecutionInfo{})),
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(0, "", "", &handler.ExecutionInfo{})),
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseSucceeding,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_SUCCEEDED, idlcore.TaskExecution_FAILED},
		},
		// TODO @hamersaw - recording failure message?
		{
			name: "OneSubNodeFailed",
			subNodePhases: []v1alpha1.NodePhase{
				v1alpha1.NodePhaseRunning,
				v1alpha1.NodePhaseRunning,
			},
			subNodeTaskPhases: []core.Phase{
				core.PhaseRunning,
				core.PhaseRunning,
			},
			subNodeTransitions: []handler.Transition{
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(0, "", "", &handler.ExecutionInfo{})),
				handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(&handler.ExecutionInfo{})),
			},
			expectedArrayNodePhase: v1alpha1.ArrayNodePhaseFailing,
			expectedTransitionPhase: handler.EPhaseRunning,
			expectedExternalResourcePhases: []idlcore.TaskExecution_Phase{idlcore.TaskExecution_FAILED, idlcore.TaskExecution_SUCCEEDED},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scope := promutils.NewTestScope()
			dataStore, err := storage.NewDataStore(&storage.Config{
				Type: storage.TypeMemory,
			}, scope)
			assert.NoError(t, err)

			// configure SubNodePhases and SubNodeTaskPhases
			arrayNodeState := &interfaces.ArrayNodeState{
				Phase: v1alpha1.ArrayNodePhaseExecuting,
			}
			for _, item := range []struct{arrayReference *bitarray.CompactArray; maxValue int}{
				{arrayReference: &arrayNodeState.SubNodePhases, maxValue: int(v1alpha1.NodePhaseRecovered)}, 
					{arrayReference: &arrayNodeState.SubNodeTaskPhases, maxValue: len(core.Phases)-1},
					{arrayReference: &arrayNodeState.SubNodeRetryAttempts, maxValue: 1},
					{arrayReference: &arrayNodeState.SubNodeSystemFailures, maxValue: 1},
				} {

				*item.arrayReference, err = bitarray.NewCompactArray(uint(size), bitarray.Item(item.maxValue))
				assert.NoError(t, err)
			}

			for i, nodePhase := range test.subNodePhases {
				arrayNodeState.SubNodePhases.SetItem(i, bitarray.Item(nodePhase))
			}
			for i, taskPhase := range test.subNodeTaskPhases {
				arrayNodeState.SubNodeTaskPhases.SetItem(i, bitarray.Item(taskPhase))
			}

			// create NodeExecutionContext
			eventRecorder := newArrayEventRecorder()

			nodeSpec := arrayNodeSpec
			nodeSpec.ArrayNode.Parallelism = uint32(test.parallelism)
			nodeSpec.ArrayNode.MinSuccessRatio = test.minSuccessRatio

			nCtx, err := createNodeExecutionContext(t, ctx, dataStore, eventRecorder, literalMap, &arrayNodeSpec, arrayNodeState)
			assert.NoError(t, err)

			// initialize ArrayNodeHandler
			nodeHandler := &mocks.NodeHandler{}
			nodeHandler.OnFinalizeRequired().Return(false)
			for i, transition := range test.subNodeTransitions {
				nodeID := fmt.Sprintf("%s-n%d", nCtx.NodeID(), i)
				transitionPhase := test.expectedExternalResourcePhases[i]

				nodeHandler.OnHandleMatch(mock.Anything, mock.MatchedBy(func(arrayNCtx interfaces.NodeExecutionContext) bool {
					return arrayNCtx.NodeID() == nodeID  // match on NodeID using index to ensure each subNode is handled independently
				})).Run(
					func(args mock.Arguments) {
						// mock sending TaskExecutionEvent from handler to show task state transition
						taskExecutionEvent := &event.TaskExecutionEvent{
							Phase: transitionPhase,
						}

						args.Get(1).(interfaces.NodeExecutionContext).EventsRecorder().RecordTaskEvent(ctx, taskExecutionEvent, &config.EventConfig{})
					},
				).Return(transition, nil)
			}

			arrayNodeHandler, err := createArrayNodeHandler(t, ctx, nodeHandler, dataStore, scope)
			assert.NoError(t, err)

			// evaluate node
			transition, err := arrayNodeHandler.Handle(ctx, nCtx)
			assert.NoError(t, err)

			// validate results
			assert.Equal(t, test.expectedArrayNodePhase, arrayNodeState.Phase)
			assert.Equal(t, test.expectedTransitionPhase, transition.Info().GetPhase())

			if len(test.expectedExternalResourcePhases) > 0 {
				assert.Equal(t, 1, len(eventRecorder.taskEvents))

				externalResources := eventRecorder.taskEvents[0].Metadata.GetExternalResources()
				assert.Equal(t, len(test.expectedExternalResourcePhases), len(externalResources))
				for i, expectedPhase := range test.expectedExternalResourcePhases {
					assert.Equal(t, expectedPhase, externalResources[i].Phase)
				}
			} else {
				assert.Equal(t, 0, len(eventRecorder.taskEvents))
			}
		})
	}
}

func TestHandleArrayNodePhaseSucceeding(t *testing.T) {
	// TODO @hamersaw - complete
}

func TestHandleArrayNodePhaseFailing(t *testing.T) {
	// TODO @hamersaw - complete
}

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}

func convertMapToArrayLiterals(values map[string][]int64) *idlcore.LiteralMap {
	literalMap := make(map[string]*idlcore.Literal)
	for k, v := range values {
		// create LiteralCollection
		literalList := make([]*idlcore.Literal, 0, len(v))
		for _, x := range v {
			literalList = append(literalList, &idlcore.Literal{
				Value: &idlcore.Literal_Scalar{
					Scalar: &idlcore.Scalar{
						Value: &idlcore.Scalar_Primitive{
							Primitive: &idlcore.Primitive{
								Value: &idlcore.Primitive_Integer{
									Integer: x,
								},
							},
						},
					},
				},
			})
		}

		// add LiteralCollection to map
		literalMap[k] = &idlcore.Literal{
			Value: &idlcore.Literal_Collection{
				Collection: &idlcore.LiteralCollection{
					Literals: literalList,
				},
			},
		}
	}

	return &idlcore.LiteralMap{
		Literals: literalMap,
	}
}
