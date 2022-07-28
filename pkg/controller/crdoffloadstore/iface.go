package crdoffloadstore

import (
	"context"
	"fmt"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	"github.com/flyteorg/flytestdlib/storage"
)

//go:generate mockery -all -output=mocks -case=underscore

// CRDOffloadStore interface provides an abstraction for retrieving StaticWorkflowData instances for
// static data that is offloaded from the FlyteWorkflow CRD.
type CRDOffloadStore interface {
	Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error)
	Remove(ctx context.Context, dataReference v1alpha1.DataReference) error
}

func NewCRDOffloadStore(ctx context.Context, cfg *Config, dataStore *storage.DataStore, scope promutils.Scope) (CRDOffloadStore, error) {
	switch cfg.Policy {
	case PolicyInMemory:
		return NewInmemoryCRDOffloadStore(NewPassthroughCRDOffloadStore(dataStore), scope), nil
	case PolicyLRU:
		return NewLRUCRDOffloadStore(NewPassthroughCRDOffloadStore(dataStore), cfg.Size, scope)
	case PolicyPassThrough:
		return NewPassthroughCRDOffloadStore(dataStore), nil
	}

	return nil, fmt.Errorf("empty crd offload store config")
}
