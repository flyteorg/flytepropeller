package nodes

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/catalog"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/flyteorg/flytestdlib/logger"

	"github.com/pkg/errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	/*"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/catalog"
	//pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/pkg/errors"
	//"google.golang.org/grpc/codes"
	//"google.golang.org/grpc/status"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	errors2 "github.com/flyteorg/flytepropeller/pkg/controller/nodes/errors"*/
)

func (n *nodeExecutor) CheckCacheCatalog(ctx context.Context, key catalog.Key) (catalog.Entry, error) {
	//logger.Infof(ctx, "Catalog CacheEnabled: Looking up catalog Cache.")
	resp, err := n.catalog.Get(ctx, key)
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

	if resp.GetStatus().GetCacheStatus() != core.CatalogCacheStatus_CACHE_HIT {
		logger.Errorf(ctx, "No CacheHIT and no Error received. Illegal state, Cache State: %s", resp.GetStatus().GetCacheStatus().String())
		// TODO should this be an error?
		return resp, nil
	}

	// TODO @hamersaw - do we need this?!?!
	//logger.Infof(ctx, "Catalog CacheHit: for task [%s/%s/%s/%s]", tk.Id.Project, tk.Id.Domain, tk.Id.Name, tk.Id.Version)
	//t.metrics.catalogHitCount.Inc(ctx)
	/*if iface := tk.Interface; iface != nil && iface.Outputs != nil && len(iface.Outputs.Variables) > 0 {
		if err := outputWriter.Put(ctx, resp.GetOutputs()); err != nil {
			logger.Errorf(ctx, "failed to write data to Storage, err: %v", err.Error())
			return catalog.Entry{}, errors.Wrapf(err, "failed to copy cached results for task.")
		}
	}*/
	// SetCached.
	return resp, nil
}

func (n *nodeExecutor) WriteCacheCatalog(ctx context.Context, key catalog.Key, outputReader io.OutputReader, metadata catalog.Metadata) (catalog.Status, error) {
	//logger.Infof(ctx, "Catalog CacheEnabled. recording execution [%s/%s/%s/%s]", tk.Id.Project, tk.Id.Domain, tk.Id.Name, tk.Id.Version)
	// ignores discovery write failures
	status, err := n.catalog.Put(ctx, key, outputReader, metadata)
	if err != nil {
		//t.metrics.catalogPutFailureCount.Inc(ctx)
		//logger.Errorf(ctx, "Failed to write results to catalog for Task [%v]. Error: %v", tk.GetId(), err2)
		return catalog.NewStatus(core.CatalogCacheStatus_CACHE_PUT_FAILURE, status.GetMetadata()), nil
	}

	return status, nil
}
