package crdoffloadstore

import (
	"context"
	"time"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type activeCRDOffloadMetrics struct {
	TotalItems   prometheus.Gauge
	CacheHit     prometheus.Counter
	CacheMiss    prometheus.Counter
	ReadError    prometheus.Counter
	FetchLatency promutils.StopWatch
}

type activeCRDOffloadStore struct {
	crdOffloadStore CRDOffloadStore
	metrics         *activeCRDOffloadMetrics
	store           map[string]*v1alpha1.StaticWorkflowData
}

func (a *activeCRDOffloadStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error) {
	location := dataReference.String()
	if m, ok := a.store[location]; ok {
		a.metrics.CacheHit.Inc()
		return m, nil
	}

	a.metrics.CacheMiss.Inc()

	timer := a.metrics.FetchLatency.Start()
	staticWorkflowData, err := a.crdOffloadStore.Get(ctx, dataReference)
	timer.Stop()
	if err != nil {
		a.metrics.ReadError.Inc()
		return nil, err
	}

	a.metrics.TotalItems.Inc()
	a.store[location] = staticWorkflowData
	return staticWorkflowData, nil
}

func (a *activeCRDOffloadStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	if err := a.crdOffloadStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	a.metrics.TotalItems.Dec()
	delete(a.store, dataReference.String())

	return nil
}

func NewActiveCRDOffloadStore(crdOffloadStore CRDOffloadStore, scope promutils.Scope) CRDOffloadStore {
	activeScope := scope.NewSubScope("active")
	metrics := &activeCRDOffloadMetrics{
		TotalItems:   activeScope.MustNewGauge("total_items", "Total Items in cache"),
		FetchLatency: activeScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:     activeScope.MustNewCounter("cache_hit", "Number of times object was found in active cache"),
		CacheMiss:    activeScope.MustNewCounter("cache_miss", "Number of times object was not found in active cache"),
		ReadError:    activeScope.MustNewCounter("cache_read_error", "Failed to read from underlying storage"),
	}

	return &activeCRDOffloadStore{
		crdOffloadStore: crdOffloadStore,
		metrics:         metrics,
		store:           map[string]*v1alpha1.StaticWorkflowData{},
	}
}
