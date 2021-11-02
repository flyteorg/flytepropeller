package manager

import (
	"context"
	"fmt"
	"time"

	managerConfig "github.com/flyteorg/flytepropeller/manager/config"
	propellerConfig "github.com/flyteorg/flytepropeller/pkg/controller/config"
	leader "github.com/flyteorg/flytepropeller/pkg/leaderelection"
	"github.com/flyteorg/flytepropeller/pkg/utils"

	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/prometheus/client_golang/prometheus"

	v1 "k8s.io/api/core/v1"

	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/leaderelection"
)

type metrics struct {
	Scope       promutils.Scope
	RoundTime   promutils.StopWatch
	PodsCreated prometheus.Counter
	PodsDeleted prometheus.Counter
	PodsRunning prometheus.Gauge
}

func newManagerMetrics(scope promutils.Scope) *metrics {
	return &metrics{
		Scope:       scope,
		RoundTime:   scope.MustNewStopWatch("round_time", "Time to perform one round of validating managed pod status'", time.Millisecond),
		PodsCreated: scope.MustNewCounter("pods_created_count", "Total number of pods created"),
		PodsDeleted: scope.MustNewCounter("pods_deleted_count", "Total number of pods deleted"),
		PodsRunning: scope.MustNewGauge("pods_running_count", "Number of managed pods currently running"),
	}
}

// The Manager periodically scans k8s to ensure liveness of multiple FlytePropeller controller
// instances and rectifies state based on the configured sharding strategy.
type Manager struct {
	kubePodsClient corev1.PodInterface
	leaderElector  *leaderelection.LeaderElector
	metrics        *metrics
	pod            *v1.Pod
	podApplication string
	scanInterval   time.Duration
	shardStrategy  ShardStrategy
}

func (m *Manager) createPods(ctx context.Context) error {
	t := m.metrics.RoundTime.Start()
	defer t.Stop()

	podNames := m.getPodNames()

	// retrieve existing pods
	podLabels := map[string]string{
		"app": m.podApplication,
	}

	listOptions := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(podLabels).String(),
	}

	pods, err := m.kubePodsClient.List(ctx, listOptions)
	if err != nil {
		return err
	}

	// note: we are unable to short-circuit if 'len(pods) == len(m.podNames)' because there may be
	// unmanaged flytepropeller pods - which is invalid configuration but will be detected later

	// determine missing managed pods
	podExists := make(map[string]bool)
	for _, podName := range podNames {
		podExists[podName] = false
	}

	podsRunning := 0
	for _, pod := range pods.Items {
		podName := pod.ObjectMeta.Name
		if _, ok := podExists[podName]; ok {
			podExists[podName] = true

			if pod.Status.Phase == v1.PodRunning {
				podsRunning++
			}
		} else {
			logger.Warnf(ctx, "detected unmanaged pod '%s'", podName)
		}
	}

	m.metrics.PodsRunning.Set(float64(podsRunning))

	// create non-existent pods
	for i, podName := range podNames {
		if exists := podExists[podName]; !exists {
			pod := m.pod.DeepCopy()
			pod.ObjectMeta.Name = podName

			err := m.shardStrategy.UpdatePodSpec(&pod.Spec, i)
			if err != nil {
				logger.Errorf(ctx, "failed to update pod spec for '%s' [%v]", podName, err)
				continue
			}

			_, err = m.kubePodsClient.Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				logger.Errorf(ctx, "failed to create pod '%s' [%v]", podName, err)
				continue
			}

			m.metrics.PodsCreated.Inc()
			logger.Infof(ctx, "created pod '%s'", podName)
		}
	}

	return nil
}

func (m *Manager) deletePods(ctx context.Context) error {
	podNames := m.getPodNames()
	for _, podName := range podNames {
		err := m.kubePodsClient.Delete(ctx, podName, metav1.DeleteOptions{})
		if err != nil {
			if kubeerrors.IsNotFound(err) {
				logger.Warnf(ctx, "deleting pod '%s' does not exist", podName)
				continue
			}

			return err
		}

		m.metrics.PodsCreated.Inc()
		logger.Infof(ctx, "deleted pod '%s'", podName)
	}

	return nil
}

func (m *Manager) getPodNames() []string {
	podCount := m.shardStrategy.GetPodCount()
	var podNames []string
	for i := 0; i < podCount; i++ {
		podNames = append(podNames, fmt.Sprintf("%s-%d", m.podApplication, i))
	}

	return podNames
}

// Runs either as a leader -if configured- or as a standalone process.
func (m *Manager) Run(ctx context.Context) error {
	if m.leaderElector != nil {
		logger.Infof(ctx, "running with leader election")
		m.leaderElector.Run(ctx)
	} else {
		logger.Infof(ctx, "running without leader election")
		if err := m.run(ctx); err != nil {
			return err
		}
		m.shutdown()
	}

	return nil
}

func (m *Manager) run(ctx context.Context) error {
	logger.Infof(ctx, "started manager")
	wait.UntilWithContext(ctx,
		func(ctx context.Context) {
			logger.Debugf(ctx, "validating managed pod(s) state")
			err := m.createPods(ctx)
			if err != nil {
				logger.Errorf(ctx, "failed to create pod(s) [%v]", err)
			}
		},
		m.scanInterval,
	)

	logger.Infof(ctx, "shutting down manager")
	return nil
}

func (m *Manager) shutdown() {
	// delete pods using a new timeout context to bound the shutdown time
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	if err := m.deletePods(ctx); err != nil {
		logger.Errorf(ctx, "failed to delete pod(s) [%v]", err)
	}
}

func New(ctx context.Context, propellerCfg *propellerConfig.Config, cfg *managerConfig.Config, kubeClient kubernetes.Interface, scope promutils.Scope) (*Manager, error) {
	shardStrategy, err := NewShardStrategy(ctx, cfg.ShardConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize shard strategy [%v]", err)
	}

	// retrieve and validate pod template
	podTemplate, err := kubeClient.CoreV1().PodTemplates(cfg.PodTemplateNamespace).Get(ctx, cfg.PodTemplateName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve pod template '%s' from namespace '%s' [%v]", cfg.PodTemplateName, cfg.PodTemplateNamespace, err)
	}

	container, err := getFlytePropellerContainer(&podTemplate.Template.Spec)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve flytepropeller container from pod template [%v]", err)
	}

	// disable leader election on all managed pods
	container.Args = append(container.Args, "--propeller.leader-election.enabled=false")

	// create singular pod spec to ensure uniformity in managed pods
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cfg.PodNamespace,
			Labels: map[string]string{
				"app": cfg.PodApplication,
			},
		},
		Spec: podTemplate.Template.Spec,
	}

	manager := &Manager{
		kubePodsClient: kubeClient.CoreV1().Pods(cfg.PodNamespace),
		metrics:        newManagerMetrics(scope),
		pod:            pod,
		podApplication: cfg.PodApplication,
		scanInterval:   cfg.ScanInterval.Duration,
		shardStrategy:  shardStrategy,
	}

	// configure leader elector
	eventRecorder, err := utils.NewK8sEventRecorder(ctx, kubeClient, "flytepropeller-manager", propellerCfg.PublishK8sEvents)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s event recorder [%v]", err)
	}

	lock, err := leader.NewResourceLock(kubeClient.CoreV1(), kubeClient.CoordinationV1(), eventRecorder, propellerCfg.LeaderElection)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize resource lock [%v]", err)
	}

	if lock != nil {
		logger.Infof(ctx, "creating leader elector for the controller")
		manager.leaderElector, err = leader.NewLeaderElector(
			lock,
			propellerCfg.LeaderElection,
			func(ctx context.Context) {
				logger.Infof(ctx, "started leading")
				if err := manager.run(ctx); err != nil {
					logger.Error(ctx, err)
				}
			},
			func() {
				// Need to check if this elector obtained leadership until k8s client-go api is fixed. Currently the
				// OnStoppingLeader func is called as a defer on every elector run, regardless of election status.
				if manager.leaderElector.IsLeader() {
					logger.Info(ctx, "stopped leading")
					manager.shutdown()
				}
			})

		if err != nil {
			return nil, fmt.Errorf("failed to initialize leader elector [%v]", err)
		}
	}

	return manager, nil
}
