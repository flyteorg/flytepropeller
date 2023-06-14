package array

import (
	"context"
	"testing"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

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

	pluginmocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io/mocks"

	"github.com/flyteorg/flytestdlib/contextutils"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/promutils/labeled"
	"github.com/flyteorg/flytestdlib/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func createArrayNodeHandler(t *testing.T, ctx context.Context, scope promutils.Scope) (interfaces.NodeHandler, error) {
	// mock components
	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	dataStore, err := storage.NewDataStore(&storage.Config{
		Type: storage.TypeMemory,
	}, scope)
	assert.NoError(t, err)
	enqueueWorkflowFunc := func(workflowID v1alpha1.WorkflowID) {}
	eventConfig := &config.EventConfig{}
	mockEventSink := eventmocks.NewMockEventSink()
	mockHandlerFactory := &mocks.HandlerFactory{}
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

func createNodeExecutionContext(t *testing.T, ctx context.Context, inputLiteralMap *idlcore.LiteralMap) (interfaces.NodeExecutionContext, error) {
	nCtx := &mocks.NodeExecutionContext{}

	// EventsRecorder
	eventRecorder := &mocks.EventRecorder{}
	eventRecorder.OnRecordTaskEventMatch(mock.Anything, mock.Anything, mock.Anything).Return(nil) // TODO @hamersaw - should probably capture to validate
	nCtx.OnEventsRecorder().Return(eventRecorder)

	// InputReader
	nCtx.OnInputReader().Return(
		newStaticInputReader(
			&pluginmocks.InputFilePaths{},
			inputLiteralMap,
		))

	// Node
	taskRef := "arrayNodeTaskID"
	nCtx.OnNode().Return(&v1alpha1.NodeSpec{
		ID: "foo",
		ArrayNode: &v1alpha1.ArrayNodeSpec{
			SubNodeSpec: &v1alpha1.NodeSpec{
				TaskRef: &taskRef,
			},
		},
	})

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
	nodeStateReader.OnGetArrayNodeState().Return(interfaces.ArrayNodeState{
		Phase: v1alpha1.ArrayNodePhaseNone,
	})
	nCtx.OnNodeStateReader().Return(nodeStateReader)

	// NodeStateWriter
	nodeStateWriter := &mocks.NodeStateWriter{}
	nodeStateWriter.OnPutArrayNodeStateMatch(mock.Anything, mock.Anything).Return(nil) // TODO @hamersaw - should probably capture to validate
	nCtx.OnNodeStateWriter().Return(nodeStateWriter)

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

	// initialize ArrayNodeHandler
	arrayNodeHandler, err := createArrayNodeHandler(t, ctx, scope)
	assert.NoError(t, err)

	tests := []struct {
		name string
		inputValues map[string][]int64
		expectedTransitionPhase handler.EPhase
	}{
		{
			name: "Success",
			inputValues: map[string][]int64{
				"foo": []int64{1, 2},
			},
			expectedTransitionPhase: handler.EPhaseRunning,
		},
		{
			name: "FailureDifferentInputListLengths",
			inputValues: map[string][]int64{
				"foo": []int64{1, 2},
				"bar": []int64{3},
			},
			expectedTransitionPhase: handler.EPhaseFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// create NodeExecutionContext
			literalMap := convertMapToArrayLiterals(test.inputValues)
			nCtx, err := createNodeExecutionContext(t, ctx, literalMap)
			assert.NoError(t, err)

			// evaluate node
			transition, err := arrayNodeHandler.Handle(ctx, nCtx)
			assert.NoError(t, err)

			// validate results
			assert.Equal(t, test.expectedTransitionPhase, transition.Info().GetPhase())
			// TODO @hamersaw - validate TaskExecutionEvent and ArrayNodeState
		})
	}
}

func TestHandleArrayNodePhaseExecuting(t *testing.T) {
	// TODO @hamersaw - complete
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
		literalList := make([]*idlcore.Literal, len(v))
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
