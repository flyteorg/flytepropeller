package crdoffloadstore

import (
	"context"
	"fmt"
	"time"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	lru "github.com/hashicorp/golang-lru"
)

type crdoffloadstoreMetrics struct {
	CacheHit       prometheus.Counter
	CacheMiss      prometheus.Counter
	CacheReadError prometheus.Counter
	FetchLatency   promutils.StopWatch
}

type lruCRDOffloadStore struct {
	cache           *lru.Cache
	crdOffloadStore CRDOffloadStore
	metrics         *crdoffloadstoreMetrics
}

func (l *lruCRDOffloadStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error) {
	// check cache for existing DataReference
	s, ok := l.cache.Get(dataReference)
	if ok {
		staticWorkflowData, ok := s.(*v1alpha1.StaticWorkflowData)
		if !ok {
			l.metrics.CacheReadError.Inc()
			return nil, fmt.Errorf("cached item in crd offload store is not expected type '*v1alpha1.StaticWorkflowData'")
		}

		l.metrics.CacheHit.Inc()
		return staticWorkflowData, nil
	}

	l.metrics.CacheMiss.Inc()

	// retrieve StaticWorkflowData from underlying CRDOffloadStore
	timer := l.metrics.FetchLatency.Start()
	staticWorkflowData, err := l.crdOffloadStore.Get(ctx, dataReference)
	timer.Stop()
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

func NewLRUCRDOffloadStore(crdOffloadStore CRDOffloadStore, size int, scope promutils.Scope) (CRDOffloadStore, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	lruScope := scope.NewSubScope("lru")
	metrics := &crdoffloadstoreMetrics{
		FetchLatency:   lruScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:       lruScope.MustNewCounter("cache_hit", "Number of times object was found in lru cache"),
		CacheMiss:      lruScope.MustNewCounter("cache_miss", "Number of times object was not found in lru cache"),
		CacheReadError: lruScope.MustNewCounter("cache_read_error", "Failed to read from lru cache"),
	}

	return &lruCRDOffloadStore{
		cache:           cache,
		crdOffloadStore: crdOffloadStore,
		metrics:         metrics,
	}, nil
}
