package manager

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/manager/config"

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
	replicaCount   int
	scanInterval   time.Duration

	// Kubernetes API.
	//metrics       *metrics
}

// TODO hamersaw - integrate prometheus metrics
/*type metrics struct {
	Scope            promutils.Scope
	EnqueueCountWf   prometheus.Counter
	EnqueueCountTask prometheus.Counter
}*/

// TODO hameraw - need to use this?
/*func getReplicaPodName(replica int) string {
	return fmt.Sprintf("%s-managed-%d", appName, replica)
}*/

func (m *Manager) extractReplicaFromString(str string) (int, error) {
	// TODO hamersaw - remove this
	replicaRegex := regexp.MustCompile(fmt.Sprintf("^%s-([0-9]+)$", m.podApplication))

	matches := replicaRegex.FindAllString(str, -1)
	if matches == nil || len(matches) != 1 {
		return -1, errors.New(fmt.Sprintf("failed to parse string '%s' with regex '%s'", str, replicaRegex))
	}

	replica, err := strconv.ParseInt(matches[0], 0, 32)
	if err != nil {
		return -1, errors.New(fmt.Sprintf("failed to parse replica value '%s' as integer", matches[0]))
	}

	return int(replica), nil
}

func (m *Manager) recoverReplicas(ctx context.Context) error {
	// TODO hamersaw - need to handle pods with Error status?
	// with 3 replicas locally we get "too many open files"
	podLabels := map[string]string{
		"app": m.podApplication,
	}

	listOptions := metav1.ListOptions{
        LabelSelector: labels.SelectorFromSet(podLabels).String(),
    }

	pods, err := m.kubePodsClient.List(ctx, listOptions)
	if err != nil {
		return err
	} else if len(pods.Items) == m.replicaCount {
		return nil
	}

	replicaExists := make(map[int]bool)
	for i := 0; i < m.replicaCount; i++ {
		replicaExists[i] = false
	}

	for _, pod := range pods.Items {
		replica, err := m.extractReplicaFromString(pod.ObjectMeta.Name)
		if err != nil {
			logger.Warnf(ctx, "failed to parse replica from pod name [%v]", err)
			continue
		} else if replica < 0 || replica >= m.replicaCount {
			logger.Warnf(ctx, "replica does not fall within valid range [0,%d)", m.replicaCount)
			continue
		}

		replicaExists[replica] = true
	}

	for replica, exists := range replicaExists {
		if !exists {

			pod := m.pod.DeepCopy()
			pod.ObjectMeta.Name = fmt.Sprintf("flytepropeller-%d", replica)

			_, err := m.kubePodsClient.Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				logger.Errorf(ctx, "failed to create replica %d [%v]", replica, err)
				continue
			}

			logger.Infof(ctx, "created pod for replica %d", replica)
		}
	}

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.scanInterval)
	defer ticker.Stop()

	go func() {
		for {
			logger.Debugf(ctx, "validating replica state")

			err := m.recoverReplicas(ctx)
			if err != nil {
				logger.Errorf(ctx, "failed to recover replicas [%v]", err)
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
	// create singular pod spec to ensure uniformity in managed pods
	podTemplate, err := kubeClient.CoreV1().PodTemplates(cfg.PodTemplateNamespace).Get(ctx, cfg.PodTemplate, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cfg.PodNamespace,
			Labels:    map[string]string{
				"app": cfg.PodApplication,
			},
		},
		Spec: podTemplate.Template.Spec,
	}

	kubePodsClient := kubeClient.CoreV1().Pods(cfg.PodNamespace)

	// TODO hamersaw - use podApplication to generate pod names
	manager := &Manager{
		kubePodsClient: kubePodsClient,
		pod:            pod,
		podApplication: cfg.PodApplication,
		replicaCount:   cfg.ReplicaCount,
		scanInterval:   cfg.ScanInterval.Duration,
	}

	return manager, nil
}
