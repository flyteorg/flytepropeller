package workflowclosurestore

import (
	"context"
	"fmt"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	"github.com/flyteorg/flytestdlib/storage"
)

//go:generate mockery -all -output=mocks -case=underscore

// WorkflowClosureStore interface provides an abstraction for retrieving CompiledWorkflowClosure blobs
// that is pushed to storage to avoid having large CRDs
type WorkflowClosureStore interface {
	Get(ctx context.Context, dataReference v1alpha1.DataReference) (*core.CompiledWorkflowClosure, error)
	Remove(ctx context.Context, dataReference v1alpha1.DataReference) error
}

func NewWorkflowClosureStore(ctx context.Context, cfg *Config, dataStore *storage.DataStore, scope promutils.Scope) (WorkflowClosureStore, error) {
	switch cfg.Policy {
	case PolicyActive:
		return NewActiveWorkflowClosureStore(NewPassthroughWorkflowClosureStore(dataStore), scope), nil
	case PolicyLRU:
		return NewLRUWorkflowClosureStore(NewPassthroughWorkflowClosureStore(dataStore), cfg.Size, scope)
	case PolicyPassThrough:
		return NewPassthroughWorkflowClosureStore(dataStore), nil
	}

	return nil, fmt.Errorf("empty workflow closure store config")
}
