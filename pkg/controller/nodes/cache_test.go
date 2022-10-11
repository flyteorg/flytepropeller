package nodes

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/datacatalog"

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
	currentAttempt       = uint32(0)
	nodeID               = "baz"
	nodeOutputDir        = storage.DataReference("output_directory")
	parentUniqueID       = "bar"
	parentCurrentAttempt = uint32(1)
	uniqueID             = "foo"
)

type mockTaskReader struct {
	taskTemplate *core.TaskTemplate
}

func (t mockTaskReader) Read(ctx context.Context) (*core.TaskTemplate, error) {
	return t.taskTemplate, nil
}
func (t mockTaskReader) GetTaskType() v1alpha1.TaskType { return "" }
func (t mockTaskReader) GetTaskID() *core.Identifier    { return nil }

func setupCacheableNodeExecutionContext(dataStore *storage.DataStore, taskTemplate *core.TaskTemplate) *nodeExecContext {
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
	mockNodeExecutionMetadata.OnGetNodeExecutionIDMatch().Return(
		&core.NodeExecutionIdentifier{
			NodeId: nodeID,
		},
	)

	var taskReader handler.TaskReader
	if taskTemplate != nil {
		taskReader = mockTaskReader{
			taskTemplate: taskTemplate,
		}
	}

	return &nodeExecContext{
		ic:         mockExecutionContext,
		md:         mockNodeExecutionMetadata,
		node:       mockNode,
		nodeStatus: mockNodeStatus,
		store:      dataStore,
		tr:         taskReader,
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
	nCtx := setupCacheableNodeExecutionContext(nil, nil)

	ownerID, err := computeCatalogReservationOwnerID(nCtx)
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
	tests := []struct {
		name                string
		cacheEntry          catalog.Entry
		cacheError          error
		catalogKey          catalog.Key
		expectedCacheStatus core.CatalogCacheStatus
		preWriteOutputFile  bool
		assertOutputFile    bool
		outputFileExists    bool
	}{
		{
			"CacheMiss",
			catalog.Entry{},
			status.Error(codes.NotFound, ""),
			catalog.Key{},
			core.CatalogCacheStatus_CACHE_MISS,
			false,
			false,
			false,
		},
		{
			"CacheHitWithOutputs",
			catalog.NewCatalogEntry(
				ioutils.NewInMemoryOutputReader(&core.LiteralMap{}, nil, nil),
				catalog.NewStatus(core.CatalogCacheStatus_CACHE_HIT, nil),
			),
			nil,
			catalog.Key{
				TypedInterface: core.TypedInterface{
					Outputs: &core.VariableMap{
						Variables: map[string]*core.Variable{
							"foo": nil,
						},
					},
				},
			},
			core.CatalogCacheStatus_CACHE_HIT,
			false,
			true,
			true,
		},
		{
			"CacheHitWithoutOutputs",
			catalog.NewCatalogEntry(
				nil,
				catalog.NewStatus(core.CatalogCacheStatus_CACHE_HIT, nil),
			),
			nil,
			catalog.Key{},
			core.CatalogCacheStatus_CACHE_HIT,
			false,
			true,
			false,
		},
		{
			"OutputsAlreadyExist",
			catalog.Entry{},
			nil,
			catalog.Key{},
			core.CatalogCacheStatus_CACHE_HIT,
			true,
			true,
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testScope := promutils.NewTestScope()

			cacheableHandler := &handlermocks.CacheableNode{}
			cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(test.catalogKey, nil)

			catalogClient := &catalogmocks.Client{}
			catalogClient.OnGetMatch(mock.Anything, mock.Anything).Return(test.cacheEntry, test.cacheError)

			dataStore, err := storage.NewDataStore(
				&storage.Config{
					Type: storage.TypeMemory,
				},
				testScope.NewSubScope("data_store"),
			)
			assert.NoError(t, err)

			nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
			nCtx := setupCacheableNodeExecutionContext(dataStore, nil)

			if test.preWriteOutputFile {
				// write mock data to outputs
				outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
				err = nCtx.DataStore().WriteProtobuf(context.TODO(), outputFile, storage.Options{}, &core.LiteralMap{})
				assert.NoError(t, err)
			}

			// execute catalog cache check
			cacheEntry, err := nodeExecutor.CheckCatalogCache(context.TODO(), nCtx, cacheableHandler)
			assert.NoError(t, err)

			// validate the result cache entry status
			assert.Equal(t, test.expectedCacheStatus, cacheEntry.GetStatus().GetCacheStatus())

			if test.assertOutputFile {
				// assert the outputs file exists
				outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
				metadata, err := nCtx.DataStore().Head(context.TODO(), outputFile)
				assert.NoError(t, err)
				assert.Equal(t, test.outputFileExists, metadata.Exists())
			}
		})
	}
}

func TestGetOrExtendCatalogReservation(t *testing.T) {
	tests := []struct {
		name                      string
		reservationOwnerID        string
		expectedReservationStatus core.CatalogReservation_Status
	}{
		{
			"Acquired",
			"bar-foo-1-baz-0",
			core.CatalogReservation_RESERVATION_ACQUIRED,
		},
		{
			"Exists",
			"some-other-owner",
			core.CatalogReservation_RESERVATION_EXISTS,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testScope := promutils.NewTestScope()

			cacheableHandler := &handlermocks.CacheableNode{}
			cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalog.Key{}, nil)

			catalogClient := &catalogmocks.Client{}
			catalogClient.OnGetOrExtendReservationMatch(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
				&datacatalog.Reservation{
					OwnerId: test.reservationOwnerID,
				},
				nil,
			)

			nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, nil, testScope)
			nCtx := setupCacheableNodeExecutionContext(nil, &core.TaskTemplate{})

			// execute catalog cache check
			reservationEntry, err := nodeExecutor.GetOrExtendCatalogReservation(context.TODO(), nCtx, cacheableHandler, time.Second*30)
			assert.NoError(t, err)

			// validate the result cache entry status
			assert.Equal(t, test.expectedReservationStatus, reservationEntry.GetStatus())
		})
	}
}

func TestReleaseCatalogReservation(t *testing.T) {
	tests := []struct {
		name                      string
		releaseError              error
		expectedReservationStatus core.CatalogReservation_Status
	}{
		{
			"Success",
			nil,
			core.CatalogReservation_RESERVATION_RELEASED,
		},
		{
			"Failure",
			errors.New("failed to release"),
			core.CatalogReservation_RESERVATION_FAILURE,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testScope := promutils.NewTestScope()

			cacheableHandler := &handlermocks.CacheableNode{}
			cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(catalog.Key{}, nil)

			catalogClient := &catalogmocks.Client{}
			catalogClient.OnReleaseReservationMatch(mock.Anything, mock.Anything, mock.Anything).Return(test.releaseError)

			nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, nil, testScope)
			nCtx := setupCacheableNodeExecutionContext(nil, &core.TaskTemplate{})

			// execute catalog cache check
			reservationEntry, err := nodeExecutor.ReleaseCatalogReservation(context.TODO(), nCtx, cacheableHandler)
			if test.releaseError == nil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}

			// validate the result cache entry status
			assert.Equal(t, test.expectedReservationStatus, reservationEntry.GetStatus())
		})
	}
}

func TestWriteCatalogCache(t *testing.T) {
	tests := []struct {
		name                string
		cacheStatus         catalog.Status
		cacheError          error
		catalogKey          catalog.Key
		expectedCacheStatus core.CatalogCacheStatus
	}{
		{
			"NoOutputs",
			catalog.NewStatus(core.CatalogCacheStatus_CACHE_DISABLED, nil),
			nil,
			catalog.Key{},
			core.CatalogCacheStatus_CACHE_DISABLED,
		},
		{
			"OutputsExist",
			catalog.NewStatus(core.CatalogCacheStatus_CACHE_POPULATED, nil),
			nil,
			catalog.Key{
				TypedInterface: core.TypedInterface{
					Outputs: &core.VariableMap{
						Variables: map[string]*core.Variable{
							"foo": nil,
						},
					},
				},
			},
			core.CatalogCacheStatus_CACHE_POPULATED,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testScope := promutils.NewTestScope()

			cacheableHandler := &handlermocks.CacheableNode{}
			cacheableHandler.OnGetCatalogKeyMatch(mock.Anything, mock.Anything).Return(test.catalogKey, nil)

			catalogClient := &catalogmocks.Client{}
			catalogClient.OnPutMatch(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(test.cacheStatus, nil)

			dataStore, err := storage.NewDataStore(
				&storage.Config{
					Type: storage.TypeMemory,
				},
				testScope.NewSubScope("data_store"),
			)
			assert.NoError(t, err)

			nodeExecutor := setupCacheableNodeExecutor(t, catalogClient, dataStore, testScope)
			nCtx := setupCacheableNodeExecutionContext(dataStore, &core.TaskTemplate{})

			// execute catalog cache check
			cacheStatus, err := nodeExecutor.WriteCatalogCache(context.TODO(), nCtx, cacheableHandler)
			assert.NoError(t, err)

			// validate the result cache entry status
			assert.Equal(t, test.expectedCacheStatus, cacheStatus.GetCacheStatus())
		})
	}
}
