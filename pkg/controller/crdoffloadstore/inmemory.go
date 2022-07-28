package crdoffloadstore

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type inmemoryCRDOffloadStore struct {
	crdOffloadStore CRDOffloadStore
	store     map[string]*v1alpha1.StaticWorkflowData
}

func (i *inmemoryCRDOffloadStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error) {
	location := dataReference.String()
	if m, ok := i.store[location]; ok {
		return m, nil
	}

	staticWorkflowData, err := i.crdOffloadStore.Get(ctx, dataReference)
	if err != nil {
		return nil, err
	}

	i.store[location] = staticWorkflowData
	return staticWorkflowData, nil
}

func (i *inmemoryCRDOffloadStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	if err := i.crdOffloadStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	delete(i.store, dataReference.String())
	return nil
}

func NewInmemoryCRDOffloadStore(crdOffloadStore CRDOffloadStore) CRDOffloadStore {
	return &inmemoryCRDOffloadStore{
		crdOffloadStore: crdOffloadStore,
		store:           map[string]*v1alpha1.StaticWorkflowData{},
	}
}
