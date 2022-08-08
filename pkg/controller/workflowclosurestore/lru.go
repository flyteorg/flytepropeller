package workflowclosurestore

import (
	"context"
	"fmt"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	lru "github.com/hashicorp/golang-lru"
)

type workflowclosurestoreMetrics struct {
	CacheHit       prometheus.Counter
	CacheMiss      prometheus.Counter
	CacheReadError prometheus.Counter
	FetchLatency   promutils.StopWatch
}

type lruWorkflowClosureStore struct {
	cache                *lru.Cache
	workflowClosureStore WorkflowClosureStore
	metrics              *workflowclosurestoreMetrics
}

func (l *lruWorkflowClosureStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*core.CompiledWorkflowClosure, error) {
	// check cache for existing DataReference
	s, ok := l.cache.Get(dataReference)
	if ok {
		workflowClosure, ok := s.(*core.CompiledWorkflowClosure)
		if !ok {
			l.metrics.CacheReadError.Inc()
			return nil, fmt.Errorf("cached item in workflow closure store is not expected type '*core.WorkflowClosure'")
		}

		l.metrics.CacheHit.Inc()
		return workflowClosure, nil
	}

	l.metrics.CacheMiss.Inc()

	// retrieve WorkflowClosure from underlying WorkflowClosureStore
	timer := l.metrics.FetchLatency.Start()
	workflowClosure, err := l.workflowClosureStore.Get(ctx, dataReference)
	timer.Stop()
	if err != nil {
		return nil, err
	}

	// add WorkflowClosure to cache and return
	l.cache.Add(dataReference, workflowClosure)
	return workflowClosure, nil
}

func (l *lruWorkflowClosureStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	// remove from underlying WorkflowClosureStore
	if err := l.workflowClosureStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	l.cache.Remove(dataReference)
	return nil
}

func NewLRUWorkflowClosureStore(workflowClosureStore WorkflowClosureStore, size int, scope promutils.Scope) (WorkflowClosureStore, error) {
	cache, err := lru.New(size)
	if err != nil {
		return nil, err
	}

	lruScope := scope.NewSubScope("lru")
	metrics := &workflowclosurestoreMetrics{
		FetchLatency:   lruScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:       lruScope.MustNewCounter("cache_hit", "Number of times object was found in lru cache"),
		CacheMiss:      lruScope.MustNewCounter("cache_miss", "Number of times object was not found in lru cache"),
		CacheReadError: lruScope.MustNewCounter("cache_read_error", "Failed to read from lru cache"),
	}

	return &lruWorkflowClosureStore{
		cache:                cache,
		workflowClosureStore: workflowClosureStore,
		metrics:              metrics,
	}, nil
}
