package wfcrdfieldsstore

import (
	"context"
	"fmt"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/flyteorg/flytestdlib/promutils"
	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
)

type lruCrdFieldsStoreMetrics struct {
	CacheHit       prometheus.Counter
	CacheMiss      prometheus.Counter
	CacheReadError prometheus.Counter
	FetchLatency   promutils.StopWatch
}

type lruWfClosureCrdFieldsStore struct {
	cache                   *lru.Cache
	wfClosureCrdFieldsStore WfClosureCrdFieldsStore
	metrics                 *lruCrdFieldsStoreMetrics
}

func (l *lruWfClosureCrdFieldsStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*k8s.WfClosureCrdFields, error) {
	// check cache for existing DataReference
	s, ok := l.cache.Get(dataReference)
	if ok {
		wfClosureCrdFields, ok := s.(*k8s.WfClosureCrdFields)
		if !ok {
			l.metrics.CacheReadError.Inc()
			return nil, fmt.Errorf("cached item in workflow closure store is not expected type '*core.WorkflowClosure'")
		}

		l.metrics.CacheHit.Inc()
		return wfClosureCrdFields, nil
	}

	l.metrics.CacheMiss.Inc()

	// retrieve wfClosureCrdFields from underlying WfClosureCrdFieldsStore
	timer := l.metrics.FetchLatency.Start()
	wfClosureCrdFields, err := l.wfClosureCrdFieldsStore.Get(ctx, dataReference)
	timer.Stop()
	if err != nil {
		return nil, err
	}

	// add WorkflowClosure to cache and return
	l.cache.Add(dataReference, wfClosureCrdFields)
	return wfClosureCrdFields, nil
}

func (l *lruWfClosureCrdFieldsStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	// remove from underlying WfClosureCrdFieldsStore
	if err := l.wfClosureCrdFieldsStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	l.cache.Remove(dataReference)
	return nil
}

func NewLRUWfClosureCrdFieldsStore(wfClosureCrdFieldsStore WfClosureCrdFieldsStore, size int, scope promutils.Scope) (WfClosureCrdFieldsStore, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	lruScope := scope.NewSubScope("lru")
	metrics := &lruCrdFieldsStoreMetrics{
		FetchLatency:   lruScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:       lruScope.MustNewCounter("cache_hit", "Number of times object was found in lru cache"),
		CacheMiss:      lruScope.MustNewCounter("cache_miss", "Number of times object was not found in lru cache"),
		CacheReadError: lruScope.MustNewCounter("cache_read_error", "Failed to read from lru cache"),
	}

	return &lruWfClosureCrdFieldsStore{
		cache:                   cache,
		wfClosureCrdFieldsStore: wfClosureCrdFieldsStore,
		metrics:                 metrics,
	}, nil
}
