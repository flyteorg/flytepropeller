package crdoffloadstore

import (
	"context"

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

func NewCRDOffloadStore(dataStore *storage.DataStore) (CRDOffloadStore, error) {
	return NewLRUCRDOffloadStore(NewPassthroughCRDOffloadStore(dataStore), 100)
	//return nil, fmt.Errorf("unimplemented")
}
