package resourceRequest

import (
	"context"
	v1alpha12 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/apis/resourceRequest/v1alpha1"
	config2 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/controller/config"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"sigs.k8s.io/controller-runtime/pkg/controller"

	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ReconcileResourceRequest reconciles a ResourceRequest resource
type ReconcileResourceRequest struct {
	client                 client.Client
	cache                  cache.Cache
	metrics                *reconcilerMetrics
	resourceRequestHandler Handler
}

type reconcilerMetrics struct {
	scope          promutils.Scope
	cacheHit       labeled.Counter
	cacheMiss      labeled.Counter
	reconcileError labeled.Counter
}

func newReconcilerMetrics(scope promutils.Scope) *reconcilerMetrics {
	reconcilerScope := scope.NewSubScope("reconciler")
	return &reconcilerMetrics{
		scope:          reconcilerScope,
		cacheHit:       labeled.NewCounter("cache_hit", "flyte resourceRequest resource fetched from cache", reconcilerScope),
		cacheMiss:      labeled.NewCounter("cache_miss", "flyte resourceRequest resource missing from cache", reconcilerScope),
		reconcileError: labeled.NewCounter("reconcile_error", "Reconcile for resourceRequest failed", reconcilerScope),
	}
}

func (r *ReconcileResourceRequest) getResource(ctx context.Context, key types.NamespacedName, obj runtime.Object) error {
	err := r.cache.Get(ctx, key, obj)
	if err != nil && isK8sObjectDoesNotExist(err) {
		r.metrics.cacheMiss.Inc(ctx)
		return r.client.Get(ctx, key, obj)
	}
	if err == nil {
		r.metrics.cacheHit.Inc(ctx)
	}
	return err
}


func (r *ReconcileResourceRequest) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx := context.Background()
	ctx = contextutils.WithNamespace(ctx, request.Namespace)
	ctx = contextutils.WithAppName(ctx, request.Name)
	typeMeta := metaV1.TypeMeta{
		Kind:      v1alpha12.ResourceRequestKind,
		APIVersion: v1alpha12.SchemeGroupVersion.String(),
	}
	// Fetch the ResourceRequest instance
	instance := &v1alpha12.ResourceRequest{
		TypeMeta: typeMeta,
	}

	err := r.getResource(ctx, request.NamespacedName, instance)
	if err != nil {
		if isK8sObjectDoesNotExist(err) {
			// Request object not found, could have been deleted after reconcile resourceRequest.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}

		return reconcile.Result{Requeue:true}, err
	}
	// We are seeing instances where getResource is removing TypeMeta
	instance.TypeMeta = typeMeta
	ctx = contextutils.WithPhase(ctx, string(instance.Status.Phase))
	requeue, err := r.resourceRequestHandler.Handle(ctx, instance)
	if err != nil {
		r.metrics.reconcileError.Inc(ctx)
		logger.Warnf(ctx, "Failed to reconcile resource %v: %v", request.NamespacedName, err)
	}

	return reconcile.Result{Requeue:requeue}, err
}

// Add creates a new ResourceRequest Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(ctx context.Context, mgr manager.Manager, cfg config2.RuntimeConfig) error {
	eventRecorder := mgr.GetEventRecorderFor(config2.AppName)

	resourceRequestHandler, err := NewResourceRequestHandler(nil, eventRecorder, cfg)
	if err != nil {
		return err
	}

	// TODO ssingh is there a better place to invoke this?
	resourceRequestHandler.processAllocationInBackground(ctx)

	metrics := newReconcilerMetrics(cfg.MetricsScope)
	reconciler := ReconcileResourceRequest{
		client:                 mgr.GetClient(),
		cache:                  mgr.GetCache(),
		metrics:                metrics,
		resourceRequestHandler: resourceRequestHandler,
	}

	c, err := controller.New(config2.AppName, mgr, controller.Options{
		MaxConcurrentReconciles: config2.GetConfig().Workers,
		Reconciler:              &reconciler,
	})

	if err != nil {
		return err
	}

	if err = c.Watch(&source.Kind{Type: &v1alpha12.ResourceRequest{}}, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}

	return nil
}

func isK8sObjectDoesNotExist(err error) bool {
	return  k8serrors.IsNotFound(err) || k8serrors.IsGone(err) || k8serrors.IsResourceExpired(err)
}

