package workflowstore

import (
	"context"
	"fmt"

	flyteworkflowv1alpha1 "github.com/lyft/flytepropeller/pkg/client/clientset/versioned/typed/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/client/listers/flyteworkflow/v1alpha1"
	"github.com/lyft/flytestdlib/promutils"
)

func NewWorkflowStore(ctx context.Context, cfg *Config, lister v1alpha1.FlyteWorkflowLister,
	workflows flyteworkflowv1alpha1.FlyteworkflowV1alpha1Interface, scope promutils.Scope) (FlyteWorkflow, error) {

	switch cfg.Type {
	case TypeInMemory:
		return NewInMemoryWorkflowStore(), nil
	case TypePassThrough:
		return NewPassthroughWorkflowStore(ctx, scope, workflows, lister), nil
	case TypeResourceVersionCache:
		if cfg.ResourceVersionCache == nil {
			return nil, fmt.Errorf("expects resource version config to be supplied when trying to instantiate resource version workflow store")
		}

		underlyingStore, err := NewWorkflowStore(ctx, cfg.ResourceVersionCache, lister, workflows, scope.NewSubScope("resourceversion"))
		if err != nil {
			return nil, err
		}

		return NewResourceVersionCachingStore(ctx, scope, underlyingStore), nil
	}

	return nil, fmt.Errorf("empty workflow store config")
}
