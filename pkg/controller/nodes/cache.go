package nodes

import (
	"context"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/catalog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/common"
	nodeserrors "github.com/flyteorg/flytepropeller/pkg/controller/nodes/errors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task"

	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/storage"

	"github.com/pkg/errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func computeCatalogReservationOwnerID(ctx context.Context, nCtx *nodeExecContext) (string, error) {
	currentNodeUniqueID, err := common.GenerateUniqueID(nCtx.ExecutionContext().GetParentInfo(), nCtx.NodeID())
	if err != nil {
		return "", err
	}

	_, ownerID, err := task.ComputeRawOutputPrefix(ctx, task.IDMaxLength, nCtx, currentNodeUniqueID, nCtx.CurrentAttempt())
	if err != nil {
		return "", err
	}

	return ownerID, nil
}

func (n *nodeExecutor) CheckCatalogCache(ctx context.Context, nCtx *nodeExecContext, cacheHandler handler.CacheableNode) (catalog.Entry, error) {
	catalogKey, err := cacheHandler.GetCatalogKey(ctx, nCtx)
	if err != nil {
		// TODO @hamersaw fail
		return catalog.Entry{}, err
	}

	//logger.Infof(ctx, "Catalog CacheEnabled: Looking up catalog Cache.")
	entry, err := n.catalog.Get(ctx, catalogKey)
	if err != nil {
		causeErr := errors.Cause(err)
		if taskStatus, ok := status.FromError(causeErr); ok && taskStatus.Code() == codes.NotFound {
			//t.metrics.catalogMissCount.Inc(ctx)
			logger.Infof(ctx, "Catalog CacheMiss: Artifact not found in Catalog. Executing Task.")
			return catalog.NewCatalogEntry(nil, catalog.NewStatus(core.CatalogCacheStatus_CACHE_MISS, nil)), nil
		}

		//t.metrics.catalogGetFailureCount.Inc(ctx)
		logger.Errorf(ctx, "Catalog Failure: memoization check failed. err: %v", err.Error())
		return catalog.Entry{}, errors.Wrapf(err, "Failed to check Catalog for previous results")
	}

	// TODO @hamersaw - figure out
	iface := catalogKey.TypedInterface
	if entry.GetStatus().GetCacheStatus() == core.CatalogCacheStatus_CACHE_HIT && iface.Outputs != nil && len(iface.Outputs.Variables) > 0 {
		// copy cached outputs to node outputs
		o, ee, err := entry.GetOutputs().Read(ctx)
		if err != nil {
			logger.Errorf(ctx, "failed to read from catalog, err: %s", err.Error())
			return catalog.Entry{}, err
		} else if ee != nil {
			logger.Errorf(ctx, "got execution error from catalog output reader? This should not happen, err: %s", ee.String())
			return catalog.Entry{}, nodeserrors.Errorf(nodeserrors.IllegalStateError, nCtx.NodeID(), "execution error from a cache output, bad state: %s", ee.String())
		}

		outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
		if err := nCtx.DataStore().WriteProtobuf(ctx, outputFile, storage.Options{}, o); err != nil {
			logger.Errorf(ctx, "failed to write cached value to datastore, err: %s", err.Error())
			return catalog.Entry{}, err
		}
	}

	// SetCached.
	//logger.Errorf(ctx, "No CacheHIT and no Error received. Illegal state, Cache State: %s", entry.GetStatus().GetCacheStatus().String())
	// TODO should this be an error?
	return entry, nil
}

// GetOrExtendCatalogReservation attempts to acquire an artifact reservation if the task is
// cachable and cache serializable. If the reservation already exists for this owner, the
// reservation is extended.
func (n *nodeExecutor) GetOrExtendCatalogReservation(ctx context.Context, nCtx *nodeExecContext,
	cacheHandler handler.CacheableNode, heartbeatInterval time.Duration) (catalog.ReservationEntry, error) {

	catalogKey, err := cacheHandler.GetCatalogKey(ctx, nCtx)
	if err != nil {
		// TODO @hamersaw fail
		return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_DISABLED), err
	}

	ownerID, err := computeCatalogReservationOwnerID(ctx, nCtx)
	if err != nil {
		// TODO @hamersaw - fail
		return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_DISABLED), err
	}

	reservation, err := n.catalog.GetOrExtendReservation(ctx, catalogKey, ownerID, heartbeatInterval)
	if err != nil {
		//t.metrics.reservationGetFailureCount.Inc(ctx)
		logger.Errorf(ctx, "Catalog Failure: reservation get or extend failed. err: %v", err.Error())
		return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_FAILURE), err
	}

	var status core.CatalogReservation_Status
	if reservation.OwnerId == ownerID {
		status = core.CatalogReservation_RESERVATION_ACQUIRED
	} else {
		status = core.CatalogReservation_RESERVATION_EXISTS
	}

	//t.metrics.reservationGetSuccessCount.Inc(ctx)
	return catalog.NewReservationEntry(reservation.ExpiresAt.AsTime(),
		reservation.HeartbeatInterval.AsDuration(), reservation.OwnerId, status), nil
}

// ReleaseCatalogReservation attempts to release an artifact reservation if the task is cachable
// and cache serializable. If the reservation does not exist for this owner (e.x. it never existed
// or has been acquired by another owner) this call is still successful.
func (n *nodeExecutor) ReleaseCatalogReservation(ctx context.Context, nCtx *nodeExecContext,
	cacheHandler handler.CacheableNode) (catalog.ReservationEntry, error) {

	catalogKey, err := cacheHandler.GetCatalogKey(ctx, nCtx)
	if err != nil {
		// TODO @hamersaw fail
		return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_DISABLED), err
	}

	ownerID, err := computeCatalogReservationOwnerID(ctx, nCtx)
	if err != nil {
		// TODO @hamersaw - fail
		return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_DISABLED), err
	}

	err = n.catalog.ReleaseReservation(ctx, catalogKey, ownerID)
	if err != nil {
		//t.metrics.reservationReleaseFailureCount.Inc(ctx)
		logger.Errorf(ctx, "Catalog Failure: release reservation failed. err: %v", err.Error())
		return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_FAILURE), err
	}

	//t.metrics.reservationReleaseSuccessCount.Inc(ctx)
	return catalog.NewReservationEntryStatus(core.CatalogReservation_RESERVATION_RELEASED), nil
}

func (n *nodeExecutor) WriteCatalogCache(ctx context.Context, nCtx *nodeExecContext, cacheHandler handler.CacheableNode) (catalog.Status, error) {
	catalogKey, err := cacheHandler.GetCatalogKey(ctx, nCtx)
	if err != nil {
		// TODO @hamersaw fail
		return catalog.NewStatus(core.CatalogCacheStatus_CACHE_DISABLED, nil), err
	}

	iface := catalogKey.TypedInterface
	if iface.Outputs != nil && len(iface.Outputs.Variables) == 0 {
		return catalog.NewStatus(core.CatalogCacheStatus_CACHE_DISABLED, nil), nil
	}

	outputPaths := ioutils.NewReadOnlyOutputFilePaths(ctx, nCtx.DataStore(), nCtx.NodeStatus().GetOutputDir())
	outputReader := ioutils.NewRemoteFileOutputReader(ctx, nCtx.DataStore(), outputPaths, nCtx.MaxDatasetSizeBytes())

	// TODO @hamersaw - need to update this once we support caching of non-tasks
	metadata := catalog.Metadata{
		TaskExecutionIdentifier: task.GetTaskExecutionIdentifier(nCtx),
	}

	//logger.Infof(ctx, "Catalog CacheEnabled. recording execution [%s/%s/%s/%s]", tk.Id.Project, tk.Id.Domain, tk.Id.Name, tk.Id.Version)
	// ignores discovery write failures
	status, err := n.catalog.Put(ctx, catalogKey, outputReader, metadata)
	if err != nil {
		//t.metrics.catalogPutFailureCount.Inc(ctx)
		//logger.Errorf(ctx, "Failed to write results to catalog for Task [%v]. Error: %v", tk.GetId(), err2)
		return catalog.NewStatus(core.CatalogCacheStatus_CACHE_PUT_FAILURE, status.GetMetadata()), nil
	}

	return status, nil
}
