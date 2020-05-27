package k8s

import (
	"context"
	"github.com/lyft/flytestdlib/logger"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/prometheus/client_golang/prometheus"
)

const resourceLevelMonitorCycleDuration = 10 * time.Second
const KindKey contextutils.Key = "kind"

// This object is responsible for emitting metrics that show the current number of Flyte workflows, cut by project and domain.
// It needs to be kicked off. The periodicity is not currently configurable because it seems unnecessary. It will also
// a timer measuring how long it takes to run each measurement cycle.
type ResourceLevelMonitor struct {
	Scope promutils.Scope

	// Meta timer - this times each collection cycle to measure how long it takes to collect the levels GaugeVec below
	CollectorTimer *promutils.StopWatchVec

	// System Observability: This is a labeled gauge that emits the current number of FlyteWorkflow objects in the informer. It is used
	// to monitor current levels. It currently only splits by project/domain, not workflow status.
	levels *prometheus.GaugeVec

	// This informer will be used to get a list of the underlying objects that we want a tally of
	sharedInformer cache.SharedIndexInformer

	// The kind here will be used to differentiate all the metrics, we'll leave out group and version for now
	gvk schema.GroupVersionKind
}

func (r *ResourceLevelMonitor) countList(ctx context.Context, objects []interface{}) map[string]int {
	// Map of namespace to counts
	counts := map[string]int{}

	// Collect all workflow metrics
	for _, v := range objects {
		metadata, err := meta.Accessor(v)
		if err != nil {
			logger.Errorf(ctx, "Error converting obj %v to an Accessor %s\n", v, err)
			continue
		}
		counts[metadata.GetNamespace()]++
	}

	return counts
}

func (r *ResourceLevelMonitor) collect(ctx context.Context) {
	// Emit gauges at the namespace layer - since these are just K8s resources, we cannot be guaranteed to have the necessary
	// information to derive project/domain
	objects := r.sharedInformer.GetStore().List()
	counts := r.countList(ctx, objects)

	// Emit labeled metrics, for each project/domain combination. This can be aggregated later with Prometheus queries.
	ctxWithKind := context.WithValue(ctx, KindKey, strings.ToLower(r.gvk.Kind))
	metricKeys := []contextutils.Key{contextutils.NamespaceKey, KindKey}
	for ns, count := range counts {
		tempContext := contextutils.WithNamespace(ctxWithKind, ns)
		gauge, err := r.levels.GetMetricWith(contextutils.Values(tempContext, metricKeys...))
		if err != nil {
			panic(err)
		}
		gauge.Set(float64(count))
	}
}

func (r *ResourceLevelMonitor) RunCollector(ctx context.Context) {
	ticker := time.NewTicker(resourceLevelMonitorCycleDuration)
	collectorCtx := contextutils.WithGoroutineLabel(ctx, "k8s-resource-level-monitor")

	go func() {
		pprof.SetGoroutineLabels(collectorCtx)
		r.sharedInformer.HasSynced()
		logger.Infof(ctx, "K8s resource collector %s has synced", r.gvk.Kind)
		for {
			select {
			case <-collectorCtx.Done():
				return
			case <-ticker.C:
				stopwatch, err := r.CollectorTimer.GetMetricWith(map[string]string{KindKey: strings.ToLower(r.gvk.Kind)})
				if err != nil {
					panic(err)
				}
				t := stopwatch.Start()
				r.collect(collectorCtx)
				t.Stop()
			}
		}
	}()
}

func NewResourceLevelMonitor(ctx context.Context, scope promutils.Scope, si cache.SharedIndexInformer, gvk schema.GroupVersionKind) *ResourceLevelMonitor {
	logger.Infof(ctx, "Launching K8s gauge emitter for kind %s", gvk.Kind)
	collectorStopWatch := scope.MustNewStopWatchVec("k8s_collection_cycle", "Measures how long it takes to run a collection", time.Millisecond, "kind")
	return &ResourceLevelMonitor{
		Scope:          scope,
		CollectorTimer: collectorStopWatch,
		levels: scope.MustNewGaugeVec("k8s_resources", "Current Stuff levels",
			KindKey, contextutils.NamespaceKey.String()),
		sharedInformer: si,
		gvk:            gvk,
	}
}
