package wfcrdfieldsstore

import (
	"context"
	"fmt"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/storage"
)

//go:generate mockery -all -output=mocks -case=underscore

// WfClosureCrdFields interface provides an abstraction for retrieving CompiledWorkflowClosure blobs
// and transforming them to the static CRD fields that it contains.
// Caching the WfClosureCrdFields instead of the CompiledWorkflowClosure to avoid the
// expensive transformations.
type WfClosureCrdFieldsStore interface {
	Get(ctx context.Context, dataReference v1alpha1.DataReference) (*k8s.WfClosureCrdFields, error)
	Remove(ctx context.Context, dataReference v1alpha1.DataReference) error
}

func NewWfClosureCrdFieldsStore(ctx context.Context, cfg *Config, dataStore *storage.DataStore, scope promutils.Scope) (WfClosureCrdFieldsStore, error) {
	switch cfg.Policy {
	case PolicyActive:
		return NewActiveWfClosureCrdFieldsStore(NewPassthroughWfClosureCrdFieldsStore(dataStore), scope), nil
	case PolicyLRU:
		return NewLRUWfClosureCrdFieldsStore(NewPassthroughWfClosureCrdFieldsStore(dataStore), cfg.Size, scope)
	case PolicyPassThrough:
		return NewPassthroughWfClosureCrdFieldsStore(dataStore), nil
	}

	return nil, fmt.Errorf("empty workflow closure store config")
}
