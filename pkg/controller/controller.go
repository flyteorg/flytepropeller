package controller

import (
	"context"

	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/catalog"

	"github.com/lyft/flytepropeller/pkg/controller/config"
	"github.com/lyft/flytepropeller/pkg/controller/workflowstore"

	"github.com/lyft/flyteidl/clients/go/admin"
	"github.com/lyft/flyteidl/clients/go/events"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/record"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	clientset "github.com/lyft/flytepropeller/pkg/client/clientset/versioned"
	flyteScheme "github.com/lyft/flytepropeller/pkg/client/clientset/versioned/scheme"
	informers "github.com/lyft/flytepropeller/pkg/client/informers/externalversions"
	"github.com/lyft/flytepropeller/pkg/controller/nodes"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/lyft/flytepropeller/pkg/controller/workflow"
)

type metrics struct {
	Scope            promutils.Scope
	EnqueueCountWf   prometheus.Counter
	EnqueueCountTask prometheus.Counter
}

// Controller is the controller implementation for FlyteWorkflow resources
type Controller struct {
	workerPool          *WorkerPool
	flyteworkflowSynced cache.InformerSynced
	workQueue           CompositeWorkQueue
	gc                  *GarbageCollector
	numWorkers          int
	workflowStore       workflowstore.FlyteWorkflow
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder      record.EventRecorder
	metrics       *metrics
	leaderElector *leaderelection.LeaderElector
}

// Runs either as a leader -if configured- or as a standalone process.
func (c *Controller) Run(ctx context.Context) error {
	if c.leaderElector == nil {
		logger.Infof(ctx, "Running without leader election.")
		return c.run(ctx)
	}

	logger.Infof(ctx, "Attempting to acquire leader lease and act as leader.")
	go c.leaderElector.Run(ctx)
	<-ctx.Done()
	return nil
}

// Start the actual work of controller (e.g. GC, consume and process queue items... etc.)
func (c *Controller) run(ctx context.Context) error {
	// Initializing WorkerPool
	logger.Info(ctx, "Initializing controller")
	if err := c.workerPool.Initialize(ctx); err != nil {
		return err
	}

	// Start the GC
	if err := c.gc.StartGC(ctx); err != nil {
		logger.Errorf(ctx, "failed to start background GC")
		return err
	}

	// Start the informer factories to begin populating the informer caches
	logger.Info(ctx, "Starting FlyteWorkflow controller")
	return c.workerPool.Run(ctx, c.numWorkers, c.flyteworkflowSynced)
}

// Called from leader elector -if configured- to start running as the leader.
func (c *Controller) onStartedLeading(ctx context.Context) {
	ctx, cancelNow := context.WithCancel(context.Background())
	logger.Infof(ctx, "Acquired leader lease.")
	go func() {
		if err := c.run(ctx); err != nil {
			logger.Panic(ctx, err)
		}
	}()

	<-ctx.Done()
	logger.Infof(ctx, "Lost leader lease.")
	cancelNow()
}

// enqueueFlyteWorkflow takes a FlyteWorkflow resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than FlyteWorkflow.
func (c *Controller) enqueueFlyteWorkflow(obj interface{}) {
	ctx := context.TODO()
	wf, ok := obj.(*v1alpha1.FlyteWorkflow)
	if !ok {
		logger.Errorf(ctx, "Received a non Workflow object")
		return
	}
	key := wf.GetK8sWorkflowID()
	logger.Infof(ctx, "==> Enqueueing workflow [%v]", key)
	c.workQueue.Add(key.String())
}

func (c *Controller) enqueueWorkflowForNodeUpdates(wID v1alpha1.WorkflowID) {
	if wID == "" {
		return
	}
	namespace, name, err := cache.SplitMetaNamespaceKey(wID)
	if err != nil {
		if _, err2 := c.workflowStore.Get(context.TODO(), namespace, name); err2 != nil {
			if workflowstore.IsNotFound(err) {
				// Workflow is not found in storage, was probably deleted, but one of the sub-objects sent an event
				return
			}
		}
		c.metrics.EnqueueCountTask.Inc()
		c.workQueue.AddToSubQueue(wID)
	}
}

func (c *Controller) getWorkflowUpdatesHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: c.enqueueFlyteWorkflow,
		UpdateFunc: func(old, new interface{}) {
			// TODO we might need to handle updates to the workflow itself.
			// Initially maybe we should not support it at all
			c.enqueueFlyteWorkflow(new)
		},
		DeleteFunc: func(obj interface{}) {
			// There is a corner case where the obj is not in fact a valid resource (it sends a DeletedFinalStateUnknown
			// object instead) -it has to do with missing some event that leads to not knowing the final state of the
			// resource. In which case, we can't use the regular metaAccessor to read obj name/namespace but should
			// instead use cache.DeletionHandling* helper functions that know how to deal with that.

			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err != nil {
				logger.Errorf(context.TODO(), "Unable to get key for deleted obj. Error[%v]", err)
				return
			}

			_, name, err := cache.SplitMetaNamespaceKey(key)
			if err != nil {
				logger.Errorf(context.TODO(), "Unable to split enqueued key into namespace/execId. Error[%v]", err)
				return
			}

			logger.Infof(context.TODO(), "Deletion triggered for %v", name)
		},
	}
}

func newControllerMetrics(scope promutils.Scope) *metrics {
	c := scope.MustNewCounterVec("wf_enqueue", "workflow enqueue count.", "type")
	return &metrics{
		Scope:            scope,
		EnqueueCountWf:   c.WithLabelValues("wf"),
		EnqueueCountTask: c.WithLabelValues("task"),
	}
}

func newK8sEventRecorder(ctx context.Context, kubeclientset kubernetes.Interface, publishK8sEvents bool) (record.EventRecorder, error) {
	// Create event broadcaster
	// Add FlyteWorkflow controller types to the default Kubernetes Scheme so Events can be
	// logged for FlyteWorkflow Controller types.
	err := flyteScheme.AddToScheme(scheme.Scheme)
	if err != nil {
		return nil, err
	}
	logger.Info(ctx, "Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logger.InfofNoCtx)
	if publishK8sEvents {
		eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	}
	return eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName}), nil
}

// NewController returns a new FlyteWorkflow controller
func New(ctx context.Context, cfg *config.Config, kubeclientset kubernetes.Interface, flytepropellerClientset clientset.Interface,
	flyteworkflowInformerFactory informers.SharedInformerFactory, kubeClient executors.Client, scope promutils.Scope) (*Controller, error) {

	var wfLauncher launchplan.Executor
	if cfg.EnableAdminLauncher {
		adminClient, err := admin.InitializeAdminClientFromConfig(ctx)
		if err != nil {
			logger.Errorf(ctx, "failed to initialize Admin client, err :%s", err.Error())
			return nil, err
		}
		wfLauncher, err = launchplan.NewAdminLaunchPlanExecutor(ctx, adminClient, cfg.DownstreamEval.Duration,
			launchplan.GetAdminConfig(), scope.NewSubScope("admin_launcher"))
		if err != nil {
			logger.Errorf(ctx, "failed to create Admin workflow Launcher, err: %v", err.Error())
			return nil, err
		}

		if err := wfLauncher.Initialize(ctx); err != nil {
			logger.Errorf(ctx, "failed to initialize Admin workflow Launcher, err: %v", err.Error())
			return nil, err
		}
	} else {
		wfLauncher = launchplan.NewFailFastLaunchPlanExecutor()
	}

	logger.Info(ctx, "Setting up event sink and recorder")
	eventSink, err := events.ConstructEventSink(ctx, events.GetConfig(ctx))
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create EventSink [%v], error %v", events.GetConfig(ctx).Type, err)
	}
	gc, err := NewGarbageCollector(cfg, scope, clock.RealClock{}, kubeclientset.CoreV1().Namespaces(), flytepropellerClientset.FlyteworkflowV1alpha1(), cfg.LimitNamespace)
	if err != nil {
		logger.Errorf(ctx, "failed to initialize GC for workflows")
		return nil, errors.Wrapf(err, "failed to initialize WF GC")
	}

	eventRecorder, err := newK8sEventRecorder(ctx, kubeclientset, cfg.PublishK8sEvents)
	if err != nil {
		logger.Errorf(ctx, "failed to event recorder %v", err)
		return nil, errors.Wrapf(err, "failed to initialize resource lock.")
	}
	controller := &Controller{
		metrics:    newControllerMetrics(scope),
		recorder:   eventRecorder,
		gc:         gc,
		numWorkers: cfg.Workers,
	}

	lock, err := newResourceLock(kubeclientset.CoreV1(), kubeclientset.CoordinationV1(), eventRecorder, cfg.LeaderElection)
	if err != nil {
		logger.Errorf(ctx, "failed to initialize resource lock.")
		return nil, errors.Wrapf(err, "failed to initialize resource lock.")
	}

	if lock != nil {
		logger.Infof(ctx, "Creating leader elector for the controller.")
		controller.leaderElector, err = newLeaderElector(lock, cfg.LeaderElection, controller.onStartedLeading, func() {
			logger.Fatal(ctx, "Lost leader state. Shutting down.")
		})

		if err != nil {
			logger.Errorf(ctx, "failed to initialize leader elector.")
			return nil, errors.Wrapf(err, "failed to initialize leader elector.")
		}
	}

	// WE are disabling this as the metrics have high cardinality. Metrics seem to be emitted per pod and this has problems
	// when we create new pods
	// Set Client Metrics Provider
	// setClientMetricsProvider(scope.NewSubScope("k8s_client"))

	// obtain references to shared index informers for FlyteWorkflow.
	flyteworkflowInformer := flyteworkflowInformerFactory.Flyteworkflow().V1alpha1().FlyteWorkflows()
	controller.flyteworkflowSynced = flyteworkflowInformer.Informer().HasSynced

	sCfg := storage.GetConfig()
	if sCfg == nil {
		logger.Errorf(ctx, "Storage configuration missing.")
	}

	store, err := storage.NewDataStore(sCfg, scope.NewSubScope("metastore"))
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create Metadata storage")
	}

	logger.Info(ctx, "Setting up Catalog client.")
	catalogClient, err := catalog.NewCatalogClient(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create datacatalog client")
	}

	workQ, err := NewCompositeWorkQueue(ctx, cfg.Queue, scope)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create WorkQueue [%v]", scope.CurrentScope())
	}
	controller.workQueue = workQ

	controller.workflowStore = workflowstore.NewPassthroughWorkflowStore(ctx, scope, flytepropellerClientset.FlyteworkflowV1alpha1(), flyteworkflowInformer.Lister())

	nodeExecutor, err := nodes.NewExecutor(ctx, cfg.DefaultDeadlines, store, controller.enqueueWorkflowForNodeUpdates, eventSink, wfLauncher, cfg.MaxDatasetSizeBytes, kubeClient, catalogClient, scope)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create Controller.")
	}

	workflowExecutor, err := workflow.NewExecutor(ctx, store, controller.enqueueWorkflowForNodeUpdates, eventSink, controller.recorder, cfg.MetadataPrefix, nodeExecutor, scope)
	if err != nil {
		return nil, err
	}

	handler := NewPropellerHandler(ctx, cfg, controller.workflowStore, workflowExecutor, scope)
	controller.workerPool = NewWorkerPool(ctx, scope, workQ, handler)

	logger.Info(ctx, "Setting up event handlers")
	// Set up an event handler for when FlyteWorkflow resources change
	flyteworkflowInformer.Informer().AddEventHandler(controller.getWorkflowUpdatesHandler())
	return controller, nil
}
