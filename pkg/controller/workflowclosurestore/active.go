package workflowclosurestore

import (
	"context"
	"time"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type activeWorkflowClosureMetrics struct {
	TotalItems   prometheus.Gauge
	CacheHit     prometheus.Counter
	CacheMiss    prometheus.Counter
	ReadError    prometheus.Counter
	FetchLatency promutils.StopWatch
}

type activeWorkflowClosureStore struct {
	workflowClosureStore WorkflowClosureStore
	metrics              *activeWorkflowClosureMetrics
	store                map[string]*core.CompiledWorkflowClosure
}

func (a *activeWorkflowClosureStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*core.CompiledWorkflowClosure, error) {
	location := dataReference.String()
	if m, ok := a.store[location]; ok {
		a.metrics.CacheHit.Inc()
		return m, nil
	}

	a.metrics.CacheMiss.Inc()

	timer := a.metrics.FetchLatency.Start()
	workflowClosure, err := a.workflowClosureStore.Get(ctx, dataReference)
	timer.Stop()
	if err != nil {
		a.metrics.ReadError.Inc()
		return nil, err
	}

	a.metrics.TotalItems.Inc()
	a.store[location] = workflowClosure
	return workflowClosure, nil
}

func (a *activeWorkflowClosureStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	if err := a.workflowClosureStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	a.metrics.TotalItems.Dec()
	delete(a.store, dataReference.String())

	return nil
}

func NewActiveWorkflowClosureStore(workflowClosureStore WorkflowClosureStore, scope promutils.Scope) WorkflowClosureStore {
	activeScope := scope.NewSubScope("active")
	metrics := &activeWorkflowClosureMetrics{
		TotalItems:   activeScope.MustNewGauge("total_items", "Total Items in cache"),
		FetchLatency: activeScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:     activeScope.MustNewCounter("cache_hit", "Number of times object was found in active cache"),
		CacheMiss:    activeScope.MustNewCounter("cache_miss", "Number of times object was not found in active cache"),
		ReadError:    activeScope.MustNewCounter("cache_read_error", "Failed to read from underlying storage"),
	}

	return &activeWorkflowClosureStore{
		workflowClosureStore: workflowClosureStore,
		metrics:              metrics,
		store:                map[string]*core.CompiledWorkflowClosure{},
	}
}
