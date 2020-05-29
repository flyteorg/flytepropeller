package k8s

import (
	"context"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/promutils"
)

const resourceLevelMonitorCycleDuration = 10 * time.Second
const KindKey contextutils.Key = "kind"

// This object is responsible for emitting metrics that show the current number of a given K8s resource kind, cut by namespace.
// It needs to be kicked off. The periodicity is not currently configurable because it seems unnecessary. It will also
// a timer measuring how long it takes to run each measurement cycle.
type ResourceLevelMonitor struct {
	Scope promutils.Scope

	// Meta timer - this times each collection cycle to measure how long it takes to collect the levels GaugeVec below
	CollectorTimer *labeled.StopWatch

	// System Observability: This is a labeled gauge that emits the current number of objects in the informer. It is used
	// to monitor current levels.
	Levels *labeled.Gauge

	// This informer will be used to get a list of the underlying objects that we want a tally of
	sharedInformer cache.SharedIndexInformer

	// The kind here will be used to differentiate all the metrics, we'll leave out group and version for now
	gvk schema.GroupVersionKind
}

// The reason that we use namespace as the one and only thing to cut by is because it's the feature that we are sure that any
// K8s resource created by a plugin will have (as yet, Flyte doesn't have any plugins that create cluster level resources and
// it probably won't for a long time).
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

// These are declared here because this constructor will be called more than once, by different K8s resource types (Pods, SparkApps, OtherCrd, etc.)
// and metric names have to be unique. It felt more reasonable at time of writing to have one metric and have each resource type just be a label
// rather than one metric per type, but can revisit this down the road.
var gauge *labeled.Gauge
var collectorStopWatch *labeled.StopWatch

func NewResourceLevelMonitor(ctx context.Context, scope promutils.Scope, si cache.SharedIndexInformer, gvk schema.GroupVersionKind) *ResourceLevelMonitor {
	logger.Infof(ctx, "Launching K8s gauge emitter for kind %s", gvk.Kind)

	// Refer to the existing labels in main.go of this repo. For these guys, we need to add namespace and kind (the K8s resource name, pod, sparkapp, etc.)
	additionalLabels := labeled.AdditionalLabelsOption{
		Labels: []string{contextutils.NamespaceKey.String(), KindKey.String()},
	}
	if gauge == nil {
		x := labeled.NewGauge("k8s_resources", "Current levels of K8s objects as seen from their informer caches", scope, additionalLabels)
		gauge = &x
	}
	if collectorStopWatch == nil {
		x := labeled.NewStopWatch("k8s_collection_cycle", "Measures how long it takes to run a collection",
			time.Millisecond, scope, additionalLabels)
		collectorStopWatch = &x
	}

	return &ResourceLevelMonitor{
		Scope:          scope,
		CollectorTimer: collectorStopWatch,
		Levels:         gauge,
		sharedInformer: si,
		gvk:            gvk,
	}
}
