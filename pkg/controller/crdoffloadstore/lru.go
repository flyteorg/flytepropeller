package crdoffloadstore

import (
	"context"
	"fmt"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	"github.com/hashicorp/golang-lru"
)

type lruCRDOffloadStore struct {
	cache           *lru.Cache
	crdOffloadStore CRDOffloadStore
}

func (l *lruCRDOffloadStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error) {
	// check cache for existing DataReference
	s, ok := l.cache.Get(dataReference)
	if ok {
		staticWorkflowData, ok := s.(*v1alpha1.StaticWorkflowData)
		if !ok {
			return nil, fmt.Errorf("cached item in crd offload store is not expected type '*v1alpha1.StaticWorkflowData'")
		}

		return staticWorkflowData, nil
	}

	// retrieve StaticWorkflowData from underlying CRDOffloadStore
	staticWorkflowData, err := l.crdOffloadStore.Get(ctx, dataReference)
	if err != nil {
		return nil, err
	}

	// add StaticWorkflowData to cache and return
	l.cache.Add(dataReference, staticWorkflowData)
	return staticWorkflowData, nil
}

func (l *lruCRDOffloadStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	// remove from underlying CRDOffloadStore
	if err := l.crdOffloadStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	l.cache.Remove(dataReference)
	return nil
}

func NewLRUCRDOffloadStore(crdOffloadStore CRDOffloadStore, size int) (CRDOffloadStore, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	return &lruCRDOffloadStore {
		cache:           cache,
		crdOffloadStore: crdOffloadStore,
	}, nil
}
