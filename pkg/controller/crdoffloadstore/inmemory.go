package crdoffloadstore

import (
	"context"
	"time"

	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type inmemoryCRDOffloadMetrics struct {
	TotalItems   prometheus.Gauge
	CacheHit     prometheus.Counter
	CacheMiss    prometheus.Counter
	ReadError    prometheus.Counter
	FetchLatency promutils.StopWatch
}

type inmemoryCRDOffloadStore struct {
	crdOffloadStore CRDOffloadStore
	store           map[string]*v1alpha1.StaticWorkflowData
	metrics         *inmemoryCRDOffloadMetrics
}

func (i *inmemoryCRDOffloadStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error) {
	location := dataReference.String()
	if m, ok := i.store[location]; ok {
		i.metrics.CacheHit.Inc()
		return m, nil
	}

	l.metrics.CacheMiss.Inc()

	timer := i.metrics.FetchLatency.Start()
	staticWorkflowData, err := i.crdOffloadStore.Get(ctx, dataReference)
	timer.Stop()
	if err != nil {
		i.metrics.ReadError.Inc()
		return nil, err
	}

	i.metrics.TotalItems.Inc()
	i.store[location] = staticWorkflowData
	return staticWorkflowData, nil
}

func (i *inmemoryCRDOffloadStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	if err := i.crdOffloadStore.Remove(ctx, dataReference); err != nil {
		return err
	}

	i.metrics.TotalItems.Dec()
	delete(i.store, dataReference.String())

	return nil
}

func NewInmemoryCRDOffloadStore(crdOffloadStore CRDOffloadStore, scope promutils.Scope) CRDOffloadStore {
	inmemoryScope := scope.NewSubScope("inmemory")
	metrics := &inmemoryCRDOffloadMetrics{
		TotalItems:   inmemoryScope.MustNewGauge("total_items", "Total Items in cache"),
		FetchLatency: inmemoryScope.MustNewStopWatch("fetch", "Total Time to read from underlying datastorage", time.Millisecond),
		CacheHit:     inmemoryScope.MustNewCounter("cache_hit", "Number of times object was found in inmemory cache"),
		CacheMiss:    inmemoryScope.MustNewCounter("cache_miss", "Number of times object was not found in inmemory cache"),
		ReadError:    inmemoryScope.MustNewCounter("cache_read_error", "Failed to read from underlying storage"),
	}

	return &inmemoryCRDOffloadStore{
		crdOffloadStore: crdOffloadStore,
		store:           map[string]*v1alpha1.StaticWorkflowData{},
		metrics:         metrics,
	}
}
