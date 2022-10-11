package nodes

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	eventsmocks "github.com/flyteorg/flytepropeller/events/mocks"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	executorsmocks "github.com/flyteorg/flytepropeller/pkg/controller/executors/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	handlermocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler/mocks"
	recoverymocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/recovery/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/catalog"
	catalogmocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/catalog/mocks"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/storage"

	"k8s.io/apimachinery/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	currentAttempt = uint32(0)
	nodeID = "baz"
	nodeOutputDir = storage.DataReference("output_directory")
	parentUniqueID = "bar"
	parentCurrentAttempt = uint32(1)
	uniqueID = "foo"
)

func setupCacheableNodeExecutionContext(dataStore *storage.DataStore) *nodeExecContext {
	mockNode := &mocks.ExecutableNode{}
	mockNode.OnGetIDMatch(mock.Anything).Return(nodeID)

	mockNodeStatus := &mocks.ExecutableNodeStatus{}
	mockNodeStatus.OnGetAttemptsMatch().Return(currentAttempt)
	mockNodeStatus.OnGetOutputDir().Return(nodeOutputDir)

	mockParentInfo := &executorsmocks.ImmutableParentInfo{}
	mockParentInfo.OnCurrentAttemptMatch().Return(parentCurrentAttempt)
	mockParentInfo.OnGetUniqueIDMatch().Return(uniqueID)

	mockExecutionContext := &executorsmocks.ExecutionContext{}
	mockExecutionContext.OnGetParentInfoMatch(mock.Anything).Return(mockParentInfo)

	mockNodeExecutionMetadata := &handlermocks.NodeExecutionMetadata{}
	mockNodeExecutionMetadata.OnGetOwnerID().Return(
		types.NamespacedName{
			Name: parentUniqueID,
		},
	)

	return &nodeExecContext{
		ic:         mockExecutionContext,
		md:         mockNodeExecutionMetadata,
		node:       mockNode,
		nodeStatus: mockNodeStatus,
		store:      dataStore,
	}
}

func setupCacheableNodeExecutor(t *testing.T, catalogClient catalog.Client, dataStore *storage.DataStore, testScope promutils.Scope) *nodeExecutor {
	ctx := context.TODO()

	adminClient := launchplan.NewFailFastLaunchPlanExecutor()
	enqueueWorkflow := func(workflowID v1alpha1.WorkflowID) {}
	eventConfig := &config.EventConfig{
		RawOutputPolicy: config.RawOutputPolicyReference,
	}
	fakeKubeClient := executorsmocks.NewFakeKubeClient()
	maxDatasetSize := int64(10)
	mockEventSink := eventsmocks.NewMockEventSink()
	nodeConfig := config.GetConfig().NodeConfig
	rawOutputPrefix := storage.DataReference("s3://bucket/")
	recoveryClient := &recoverymocks.Client{}
	testClusterID := "cluster1"

	nodeExecutorInterface, err := NewExecutor(ctx, nodeConfig, dataStore, enqueueWorkflow, mockEventSink,
		adminClient, adminClient, maxDatasetSize, rawOutputPrefix, fakeKubeClient, catalogClient,
		recoveryClient, eventConfig, testClusterID, testScope.NewSubScope("node_executor"))
	assert.NoError(t, err)

	nodeExecutor, ok := nodeExecutorInterface.(*nodeExecutor)
	assert.True(t, ok)

	return nodeExecutor
}

func TestComputeCatalogReservationOwnerID(t *testing.T) {
	nCtx := setupCacheableNodeExecutionContext(nil)

	ownerID, err := computeCatalogReservationOwnerID(context.TODO(), nCtx)
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("%s-%s-%d-%s-%d", parentUniqueID, uniqueID, parentCurrentAttempt, nodeID, currentAttempt), ownerID)
}

func TestUpdatePhaseCacheInfo(t *testing.T) {
	cacheStatus := catalog.NewStatus(core.CatalogCacheStatus_CACHE_MISS, nil)
	reservationStatus := core.CatalogReservation_RESERVATION_EXISTS

	tests := []struct {
		name              string
		cacheStatus       *catalog.Status
		reservationStatus *core.CatalogReservation_Status
	}{
		{"BothEmpty", nil, nil},
		{"CacheStatusOnly", &cacheStatus, nil},
		{"ReservationStatusOnly", nil, &reservationStatus},
		{"BothPopulated", &cacheStatus, &reservationStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			phaseInfo := handler.PhaseInfoUndefined
			phaseInfo = updatePhaseCacheInfo(phaseInfo, test.cacheStatus, test.reservationStatus)

			// do not create ExecutionInfo object if neither cacheStatus or reservationStatus exists
			if test.cacheStatus == nil && test.reservationStatus == nil {
				assert.Nil(t, phaseInfo.GetInfo())
			}

			// ensure cache and reservation status' are being set correctly
			if test.cacheStatus != nil {
				assert.Equal(t, cacheStatus.GetCacheStatus(), phaseInfo.GetInfo().TaskNodeInfo.TaskNodeMetadata.CacheStatus)
			}

			if test.reservationStatus != nil {
				assert.Equal(t, reservationStatus, phaseInfo.GetInfo().TaskNodeInfo.TaskNodeMetadata.ReservationStatus)
			}
		})
	}
}

func TestCheckCatalogCache(t *testing.T) {
	t.Run("CacheMiss", func(t *testing.T) {
		testScope := promutils.NewTestScope()

		cacheableHandler := &handlermocks.CacheableNode{}
		cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalog.Key{}, nil)

		catalogClient := &catalogmocks.Client{}
		catalogClient.OnGetMatch(mock.Anything, mock.Anything).Return(catalog.Entry{}, status.Error(codes.NotFound, ""))

		dataStore, err := storage.NewDataStore(
			&storage.Config{
				Type: storage.TypeMemory,
			},
			testScope.NewSubScope("data_store"),
		)
		assert.NoError(t, err)

		nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
		nCtx := setupCacheableNodeExecutionContext(dataStore)

		resultCacheEntry, err := nodeExecutor.CheckCatalogCache(context.TODO(), nCtx, cacheableHandler)
		assert.NoError(t, err)

		assert.Equal(t, core.CatalogCacheStatus_CACHE_MISS, resultCacheEntry.GetStatus().GetCacheStatus())
	})

	t.Run("CacheHitWithOutputs", func(t *testing.T) {
		testScope := promutils.NewTestScope()
		cacheEntry := catalog.NewCatalogEntry(
			ioutils.NewInMemoryOutputReader(&core.LiteralMap{}, nil, nil),
			catalog.NewStatus(core.CatalogCacheStatus_CACHE_HIT, nil),
		)
		catalogKey := catalog.Key{
			TypedInterface: core.TypedInterface{
				Outputs: &core.VariableMap{
					Variables: map[string]*core.Variable{
						"foo": nil,
					},
				},
			},
		}

		cacheableHandler := &handlermocks.CacheableNode{}
		cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalogKey, nil)

		catalogClient := &catalogmocks.Client{}
		catalogClient.OnGetMatch(mock.Anything, mock.Anything).Return(cacheEntry, nil)

		dataStore, err := storage.NewDataStore(
			&storage.Config{
				Type: storage.TypeMemory,
			},
			testScope.NewSubScope("data_store"),
		)
		assert.NoError(t, err)

		nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
		nCtx := setupCacheableNodeExecutionContext(dataStore)

		resultCacheEntry, err := nodeExecutor.CheckCatalogCache(context.TODO(), nCtx, cacheableHandler)
		assert.NoError(t, err)

		assert.Equal(t, core.CatalogCacheStatus_CACHE_HIT, resultCacheEntry.GetStatus().GetCacheStatus())

		// assert the outputs file exists
		outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
		metadata, err := nCtx.DataStore().Head(context.TODO(), outputFile)
		assert.NoError(t, err)
		assert.Equal(t, true, metadata.Exists())
	})

	t.Run("CacheHitWithoutOutputs", func(t *testing.T) {
		testScope := promutils.NewTestScope()
		cacheEntry := catalog.NewCatalogEntry(nil, catalog.NewStatus(core.CatalogCacheStatus_CACHE_HIT, nil))

		cacheableHandler := &handlermocks.CacheableNode{}
		cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalog.Key{}, nil)

		catalogClient := &catalogmocks.Client{}
		catalogClient.OnGetMatch(mock.Anything, mock.Anything).Return(cacheEntry, nil)

		dataStore, err := storage.NewDataStore(
			&storage.Config{
				Type: storage.TypeMemory,
			},
			testScope.NewSubScope("data_store"),
		)
		assert.NoError(t, err)

		nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
		nCtx := setupCacheableNodeExecutionContext(dataStore)

		resultCacheEntry, err := nodeExecutor.CheckCatalogCache(context.TODO(), nCtx, cacheableHandler)
		assert.NoError(t, err)

		assert.Equal(t, core.CatalogCacheStatus_CACHE_HIT, resultCacheEntry.GetStatus().GetCacheStatus())

		// assert the outputs file does not exist
		outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
		metadata, err := nCtx.DataStore().Head(context.TODO(), outputFile)
		assert.NoError(t, err)
		assert.Equal(t, false, metadata.Exists())
	})

	t.Run("CacheLookupError", func(t *testing.T) {
		testScope := promutils.NewTestScope()

		cacheableHandler := &handlermocks.CacheableNode{}
		cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalog.Key{}, nil)

		catalogClient := &catalogmocks.Client{}
		catalogClient.OnGetMatch(mock.Anything, mock.Anything).Return(catalog.Entry{}, errors.New("foo"))

		dataStore, err := storage.NewDataStore(
			&storage.Config{
				Type: storage.TypeMemory,
			},
			testScope.NewSubScope("data_store"),
		)
		assert.NoError(t, err)

		nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
		nCtx := setupCacheableNodeExecutionContext(dataStore)

		_, err = nodeExecutor.CheckCatalogCache(context.TODO(), nCtx, cacheableHandler)
		assert.Error(t, err)
	})

	t.Run("OutputsAlreadyExist", func(t *testing.T) {
		testScope := promutils.NewTestScope()

		cacheableHandler := &handlermocks.CacheableNode{}
		cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalog.Key{}, nil)

		catalogClient := &catalogmocks.Client{}
		catalogClient.OnGetMatch(mock.Anything, mock.Anything).Return(catalog.Entry{}, nil)

		dataStore, err := storage.NewDataStore(
			&storage.Config{
				Type: storage.TypeMemory,
			},
			testScope.NewSubScope("data_store"),
		)
		assert.NoError(t, err)

		nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
		nCtx := setupCacheableNodeExecutionContext(dataStore)

		// write mock data to outputs
		outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
		err = nCtx.DataStore().WriteProtobuf(context.TODO(), outputFile, storage.Options{}, &core.LiteralMap{})
		assert.NoError(t, err)

		resultCacheEntry, err := nodeExecutor.CheckCatalogCache(context.TODO(), nCtx, cacheableHandler)
		assert.NoError(t, err)

		assert.Equal(t, core.CatalogCacheStatus_CACHE_HIT, resultCacheEntry.GetStatus().GetCacheStatus())
	})
}

func TestGetOrExtendCatalogReservation(t *testing.T) {
	//func (n *nodeExecutor) GetOrExtendCatalogReservation(ctx context.Context, nCtx *nodeExecContext,
	//	cacheHandler handler.CacheableNode, heartbeatInterval time.Duration) (catalog.ReservationEntry, error) {
}

func TestReleaseCatalogReservation(t *testing.T) {
	//func (n *nodeExecutor) ReleaseCatalogReservation(ctx context.Context, nCtx *nodeExecContext,
	//	cacheHandler handler.CacheableNode) (catalog.ReservationEntry, error) {
}

func TestWriteCatalogCache(t *testing.T) {
	//func (n *nodeExecutor) WriteCatalogCache(ctx context.Context, nCtx *nodeExecContext, cacheHandler handler.CacheableNode) (catalog.Status, error) {
}
