package nodes

import (
	"context"
	"testing"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1/mocks"
	executorsmocks "github.com/flyteorg/flytepropeller/pkg/controller/executors/mocks"
	handlermocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler/mocks"

	"k8s.io/apimachinery/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestComputeCatalogReservationOwnerID(t *testing.T) {
	currentAttempt := uint32(0)
	parentUniqueID := "bar"
	parentCurrentAttempt := uint32(1)
	uniqueID := "foo"

	mockNode := &mocks.ExecutableNode{}
	mockNode.OnGetIDMatch(mock.Anything).Return("baz")

	mockNodeStatus := &mocks.ExecutableNodeStatus{}
	mockNodeStatus.OnGetAttemptsMatch().Return(currentAttempt)

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

	nCtx := &nodeExecContext{
		ic: mockExecutionContext,
		md: mockNodeExecutionMetadata,
		node: mockNode,
		nodeStatus: mockNodeStatus,
	}

	ownerID, err := computeCatalogReservationOwnerID(context.TODO(), nCtx)
	assert.NoError(t, err)
	assert.Equal(t, "bar-foo-1-baz-0", ownerID)
}

func TestUpdatePhaseCacheInfo(t *testing.T) {
	//func updatePhaseCacheInfo(phaseInfo handler.PhaseInfo, cacheStatus *catalog.Status, reservationStatus *core.CatalogReservation_Status) handler.PhaseInfo {
}

func TestCheckCatalogCache(t *testing.T) {
	//func (n *nodeExecutor) CheckCatalogCache(ctx context.Context, nCtx *nodeExecContext, cacheHandler handler.CacheableNode) (catalog.Entry, error) {
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
