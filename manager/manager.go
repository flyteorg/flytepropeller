package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/flyteorg/flytepropeller/manager/config"

	"github.com/flyteorg/flytestdlib/logger"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type Manager struct {
	kubePodsClient corev1.PodInterface
	pod            *v1.Pod
	podApplication string
	podNames       []string
	scanInterval   time.Duration
	shardStrategy  ShardStrategy

	// Kubernetes API.
	//metrics       *metrics
}

// TODO hamersaw - integrate prometheus metrics
/*type metrics struct {
	Scope            promutils.Scope
	EnqueueCountWf   prometheus.Counter
	EnqueueCountTask prometheus.Counter
}*/

func (m *Manager) recoverPods(ctx context.Context) error {
	// TODO hamersaw - do we need to handle pods with Error status?
	// with 3 pods locally we get "too many open files"

	// retrieve existing pods
	// TODO hamersaw - use existing LabelSelector API (AddLabel...)
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
	for _, podName := range m.podNames {
		podExists[podName] = false
	}

	for _, pod := range pods.Items {
		podName := pod.ObjectMeta.Name
		if _, ok := podExists[podName]; ok {
			podExists[podName] = true
		} else {
			logger.Warnf(ctx, "detected unmanaged pod '%s'", podName)
		}
	}

	// create non-existent pods
	for i, podName := range m.podNames {
		if exists, _ := podExists[podName]; !exists {
			pod := m.pod.DeepCopy()
			pod.ObjectMeta.Name = podName

			err := m.shardStrategy.UpdatePodSpec(&pod.Spec, i)
			if err != nil {
				logger.Errorf(ctx, "failed to update pod spec for '%s' [%v]", podName, err)
				continue
			}

			// TODO hamersaw - tmp
			/*for _, container := range pod.Spec.Containers {
				fmt.Printf("CONTAINER: %v\n", container.Image)
				for _, arg := range container.Args {
					fmt.Printf("  %s\n", arg)
				}
			}*/

			_, err = m.kubePodsClient.Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				logger.Errorf(ctx, "failed to create pod '%s' [%v]", podName, err)
				continue
			}

			logger.Infof(ctx, "created pod '%s'", podName)
		}
	}

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.scanInterval)
	defer ticker.Stop()

	go func() {
		for {
			logger.Debugf(ctx, "validating managed pod(s) state")
			err := m.recoverPods(ctx)
			if err != nil {
				logger.Errorf(ctx, "failed to recover pods [%v]", err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
	}()

	logger.Info(ctx, "started manager")
	<-ctx.Done()
	logger.Info(ctx, "shutting down manager")

	return nil
}

func New(ctx context.Context, cfg *config.Config, kubeClient kubernetes.Interface) (*Manager, error) {
	shardStrategy, err := NewShardStrategy(ctx, cfg.ShardConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to intialize shard strategy [%v]", err)
	}

	// retrieve and validate pod template
	podTemplate, err := kubeClient.CoreV1().PodTemplates(cfg.PodTemplateNamespace).Get(ctx, cfg.PodTemplate, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if _, err := getFlytePropellerContainer(&podTemplate.Template.Spec); err != nil {
		return nil, err
	}

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

	// generate pod names
	podCount, err := shardStrategy.GetPodCount()
	if err != nil {
		return nil, err
	}

	var podNames []string
	for i := 0; i < podCount; i++ {
		podNames = append(podNames, fmt.Sprintf("%s-%d", cfg.PodApplication, i))
	}

	manager := &Manager{
		kubePodsClient: kubeClient.CoreV1().Pods(cfg.PodNamespace),
		pod:            pod,
		podApplication: cfg.PodApplication,
		podNames:       podNames,
		scanInterval:   cfg.ScanInterval.Duration,
		shardStrategy:  shardStrategy,
	}

	return manager, nil
}
