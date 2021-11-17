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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
)

const (
	podTemplateResourceVersion = "podTemplateResourceVersion"
	shardConfigHash = "shardConfigHash"
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
	kubeClient           kubernetes.Interface
	leaderElector        *leaderelection.LeaderElector
	metrics              *metrics
	podApplication       string
	podNamespace         string
	podTemplateName      string
	podTemplateNamespace string
	scanInterval         time.Duration
	shardStrategy        ShardStrategy
}

func getPodTemplate(ctx context.Context, kubeClient kubernetes.Interface, podTemplateName, podTemplateNamespace string) (*v1.PodTemplate, error) {
	podTemplate, err := kubeClient.CoreV1().PodTemplates(podTemplateNamespace).Get(ctx, podTemplateName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve pod template '%s' from namespace '%s' [%v]", podTemplateName, podTemplateNamespace, err)
	}

	return podTemplate, nil
}

func (m *Manager) createPods(ctx context.Context) error {
	t := m.metrics.RoundTime.Start()
	defer t.Stop()

	// retrieve pod list / create metadata
	podTemplate, err := getPodTemplate(ctx, m.kubeClient, m.podTemplateName, m.podTemplateNamespace)
	if err != nil {
		return err
	}

	shardConfigHash, err  := m.shardStrategy.HashCode()
	if err != nil {
		return err
	}

	podAnnotations := map[string]string{
		"podTemplateResourceVersion": podTemplate.ObjectMeta.ResourceVersion,
		"shardConfigHash":            fmt.Sprintf("%d", shardConfigHash),
	}
	podNames := m.getPodNames()
	podLabels := map[string]string{
		"app": m.podApplication,
	}

	// disable leader election on all managed pods
	container, err := getFlytePropellerContainer(&podTemplate.Template.Spec)
	if err != nil {
		return fmt.Errorf("failed to retrieve flytepropeller container from pod template [%v]", err)
	}

	container.Args = append(container.Args, "--propeller.leader-election.enabled=false")

	// retrieve existing pods
	listOptions := metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(podLabels).String(),
	}

	pods, err := m.kubeClient.CoreV1().Pods(m.podNamespace).List(ctx, listOptions)
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

		// validate existing pod annotations
		validAnnotations := true
		for key, value := range podAnnotations {
			if pod.ObjectMeta.Annotations[key] != value {
				validAnnotations = false
				break
			}
		}

		if !validAnnotations {
			// delete stale pod
			err := m.kubeClient.CoreV1().Pods(m.podNamespace).Delete(ctx, podName, metav1.DeleteOptions{})
			if err != nil {
				return err
			}

			m.metrics.PodsDeleted.Inc()
			logger.Infof(ctx, "deleted pod '%s'", podName)
			continue
		}

		// update podExists to track existing pods
		if _, ok := podExists[podName]; ok {
			podExists[podName] = true

			if pod.Status.Phase == v1.PodRunning {
				podsRunning++
			} else if pod.Status.Phase == v1.PodFailed {
				logger.Warnf(ctx, "flytepropeller pod '%s' in 'failed' state", podName)
			}
		}
	}

	m.metrics.PodsRunning.Set(float64(podsRunning))

	// create non-existent pods
	for i, podName := range podNames {
		if exists := podExists[podName]; !exists {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: podAnnotations,
					Name:        podName,
					Namespace:   m.podNamespace,
					Labels:      podLabels,
				},
				Spec: *podTemplate.Template.Spec.DeepCopy(), // TODO - ensure the * is correct
			}

			err := m.shardStrategy.UpdatePodSpec(&pod.Spec, i)
			if err != nil {
				logger.Errorf(ctx, "failed to update pod spec for '%s' [%v]", podName, err)
				continue
			}

			_, err = m.kubeClient.CoreV1().Pods(m.podNamespace).Create(ctx, pod, metav1.CreateOptions{})
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

// Creates a new FlytePropeller Manager instance
func New(ctx context.Context, propellerCfg *propellerConfig.Config, cfg *managerConfig.Config, kubeClient kubernetes.Interface, scope promutils.Scope) (*Manager, error) {
	shardStrategy, err := NewShardStrategy(ctx, cfg.ShardConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize shard strategy [%v]", err)
	}

	manager := &Manager{
		kubeClient:           kubeClient,
		metrics:              newManagerMetrics(scope),
		podApplication:       cfg.PodApplication,
		podNamespace:         cfg.PodNamespace,
		podTemplateName:      cfg.PodTemplateName,
		podTemplateNamespace: cfg.PodTemplateNamespace,
		scanInterval:         cfg.ScanInterval.Duration,
		shardStrategy:        shardStrategy,
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
				}
			})

		if err != nil {
			return nil, fmt.Errorf("failed to initialize leader elector [%v]", err)
		}
	}

	return manager, nil
}
