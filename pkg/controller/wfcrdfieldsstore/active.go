package wfcrdfieldsstore

import (
	"context"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"
)

type activeCrdFieldsMetrics struct {
	TotalItems   prometheus.Gauge
	CacheHit     prometheus.Counter
	CacheMiss    prometheus.Counter
	ReadError    prometheus.Counter
	FetchLatency promutils.StopWatch
}

type activeWfClosureCrdFieldsStore struct {
	wfClosureCrdFieldsStore WfClosureCrdFieldsStore
	metrics                 *activeCrdFieldsMetrics
	store                   map[string]*k8s.WfClosureCrdFields
}

func (a *activeWfClosureCrdFieldsStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*k8s.WfClosureCrdFields, error) {
	location := dataReference.String()
	if m, ok := a.store[location]; ok {
		a.metrics.CacheHit.Inc()
		return m, nil
	}

	a.metrics.CacheMiss.Inc()

	timer := a.metrics.FetchLatency.Start()
	wfClosureCrdFields, err := a.wfClosureCrdFieldsStore.Get(ctx, dataReference)
	timer.Stop()
	if err != nil {
		a.metrics.ReadError.Inc()
		return nil, err
	}

	a.metrics.TotalItems.Inc()
	a.store[location] = wfClosureCrdFields
	return wfClosureCrdFields, nil
}

func (a *activeWfClosureCrdFieldsStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	if err := a.wfClosureCrdFieldsStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	a.metrics.TotalItems.Dec()
	delete(a.store, dataReference.String())

	return nil
}

func NewActiveWfClosureCrdFieldsStore(wfClosureCrdFieldsStore WfClosureCrdFieldsStore, scope promutils.Scope) WfClosureCrdFieldsStore {
	activeScope := scope.NewSubScope("active")
	metrics := &activeCrdFieldsMetrics{
		TotalItems:   activeScope.MustNewGauge("total_items", "Total Items in cache"),
		FetchLatency: activeScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:     activeScope.MustNewCounter("cache_hit", "Number of times object was found in active cache"),
		CacheMiss:    activeScope.MustNewCounter("cache_miss", "Number of times object was not found in active cache"),
		ReadError:    activeScope.MustNewCounter("cache_read_error", "Failed to read from underlying storage"),
	}

	return &activeWfClosureCrdFieldsStore{
		wfClosureCrdFieldsStore: wfClosureCrdFieldsStore,
		metrics:                 metrics,
		store:                   map[string]*k8s.WfClosureCrdFields{},
	}
}
