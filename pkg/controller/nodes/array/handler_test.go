package array

import (
	"context"
	"testing"

	"github.com/flyteorg/flytepropeller/events"
	eventmocks "github.com/flyteorg/flytepropeller/events/mocks"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	execmocks "github.com/flyteorg/flytepropeller/pkg/controller/executors/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	gatemocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes"
	gatemocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/gate/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	recoverymocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/recovery/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/catalog"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/storage"
)

func createArrayNodeExecutor(t *testing.T, ctx context.Context, scope promutils.Scope) (handler.Node, error) {
	// mock components
	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	dataStore, err := storage.NewDataStore(&storage.Config{
		Type: storage.TypeMemory,
	}, scope)
	assert.NoError(t, err)
	enqueueWorkflowFunc := func(workflowID v1alpha1.WorkflowID) {}
	eventConfig := &events.EventConfig{}
	mockEventSink = eventmocks.NewMockEventSink()
	mockKubeClient = execmocks.NewFakeKubeClient()
	mockRecoveryClient = &recoverymocks.Client{}
	mockSignalClient = &gatemocks.SignalServiceClient{}
	noopCatalogClient = catalog.NOOPCatalog{}
	scope := promutils.NewTestScope()

	// create node executor
	nodeExecutor, err := nodes.NewExecutor(ctx, config.GetConfig().NodeConfig, dataStore, enqueueWorkflowFunc, mockEventSink, adminClient,
		adminClient, 10, "s3://bucket/", mockKubeClient, noopCatalogClient, mockRecoveryClient, eventConfig, "clusterID", mockSignalClient, scope)
	assert.NoError(t, err)

	return New(nodeExecutor, eventConfig, scope)
}

func TestAbort(t *testing.T) {
	// TODO @hamersaw - complete
}

func TestFinalize(t *testing.T) {
	// TODO @hamersaw - complete
}

func TestHandleArrayNodePhaseNone(t *testing.T) {
	ctx := context.Background()
	// TODO @hamersaw - complete
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
