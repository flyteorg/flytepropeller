package k8s

import (
	"context"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils"
)

const resourceLevelMonitorCycleDuration = 10 * time.Second
const KindKey contextutils.Key = "kind"

// This object is responsible for emitting metrics that show the current number of Flyte workflows, cut by project and domain.
// It needs to be kicked off. The periodicity is not currently configurable because it seems unnecessary. It will also
// a timer measuring how long it takes to run each measurement cycle.
type ResourceLevelMonitor struct {
	Scope promutils.Scope

	// Meta timer - this times each collection cycle to measure how long it takes to collect the levels GaugeVec below
	CollectorTimer *labeled.StopWatch

	// System Observability: This is a labeled gauge that emits the current number of FlyteWorkflow objects in the informer. It is used
	// to monitor current levels. It currently only splits by project/domain, not workflow status.
	Levels *labeled.Gauge

	// This informer will be used to get a list of the underlying objects that we want a tally of
	sharedInformer cache.SharedIndexInformer

	// The kind here will be used to differentiate all the metrics, we'll leave out group and version for now
	gvk schema.GroupVersionKind
}

func (r *ResourceLevelMonitor) countList(ctx context.Context, objects []interface{}) map[string]int {
	// Map of namespace to counts
	counts := map[string]int{}

	// Collect the object counts by namespace
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

// The context here is expected to already have a value for the KindKey
func (r *ResourceLevelMonitor) collect(ctx context.Context) {
	// Emit gauges at the namespace layer - since these are just K8s resources, we cannot be guaranteed to have the necessary
	// information to derive project/domain
	objects := r.sharedInformer.GetStore().List()
	counts := r.countList(ctx, objects)

	for ns, count := range counts {
		withNamespaceCtx := contextutils.WithNamespace(ctx, ns)
		r.Levels.Set(withNamespaceCtx, float64(count))
	}
}

func (r *ResourceLevelMonitor) RunCollector(ctx context.Context) {
	ticker := time.NewTicker(resourceLevelMonitorCycleDuration)
	collectorCtx := contextutils.WithGoroutineLabel(ctx, "k8s-resource-level-monitor")
	// Since the idea is that one of these objects is always only responsible for exactly one type of K8s resource, we
	// can safely set the context here for that kind for all downstream usage
	collectorCtx = context.WithValue(ctx, KindKey, strings.ToLower(r.gvk.Kind))

	go func() {
		pprof.SetGoroutineLabels(collectorCtx)
		r.sharedInformer.HasSynced()
		logger.Infof(ctx, "K8s resource collector %s has synced", r.gvk.Kind)
		for {
			select {
			case <-collectorCtx.Done():
				return
			case <-ticker.C:
				t := r.CollectorTimer.Start(collectorCtx)
				r.collect(collectorCtx)
				t.Stop()
			}
		}
	}()
}

func NewResourceLevelMonitor(ctx context.Context, scope promutils.Scope, si cache.SharedIndexInformer, gvk schema.GroupVersionKind) *ResourceLevelMonitor {
	logger.Infof(ctx, "Launching K8s gauge emitter for kind %s", gvk.Kind)

	// Refer to the existing labels in main.go of this repo. For these guys, we need to add namespace and kind (the K8s resource name, pod, sparkapp, etc.)
	additionalLabels := labeled.AdditionalLabelsOption{
		Labels: []string{contextutils.NamespaceKey.String(), KindKey.String()},
	}
	gauge := labeled.NewGauge("k8s_resources", "Current levels of K8s objects as seen from their informer caches", scope, additionalLabels)
	collectorStopWatch := labeled.NewStopWatch("k8s_collection_cycle", "Measures how long it takes to run a collection",
		time.Millisecond, scope, additionalLabels)

	return &ResourceLevelMonitor{
		Scope:          scope,
		CollectorTimer: &collectorStopWatch,
		Levels:         &gauge,
		sharedInformer: si,
		gvk:            gvk,
	}
}
