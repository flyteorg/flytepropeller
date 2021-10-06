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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

var replicaRegex = regexp.MustCompile("^flytepropeller-([1-9]+[0-9]*)$")

type Manager struct {
	kubeClient   kubernetes.Interface
	namespace    string
	podTemplate  *corev1.PodTemplate
	replicaCount int
	scanInterval time.Duration

	// Kubernetes API.
	//metrics       *metrics
}

/*type metrics struct {
	Scope            promutils.Scope
	EnqueueCountWf   prometheus.Counter
	EnqueueCountTask prometheus.Counter
}*/

func extractReplicaFromString(str string) (int, error) {
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
	// TODO hamersaw - move these outside
	labelMap := map[string]string{
		"app": "flytepropeller",
	}

	options := metav1.ListOptions{
        LabelSelector: labels.SelectorFromSet(labelMap).String(),
    }

	pods, err := m.kubeClient.CoreV1().Pods(m.namespace).List(ctx, options)
	if err != nil {
		return err
	} else if len(pods.Items) == m.replicaCount {
		return nil
	}

	replicaExists := make(map[int]bool)
	for i := 0; i < m.replicaCount; i++ {
		replicaExists[i] = false
	}

	// find missing pod and start
	for _, pod := range pods.Items {
		replica, err := extractReplicaFromString(pod.ObjectMeta.Name)
		if err != nil {
			logger.Warnf(ctx, "failed to parse replica from pod name: [%v]", err)
			continue
		} else if replica < 0 || replica >= m.replicaCount {
			logger.Warnf(ctx, "replica does not fall within valid range [0,%d)", m.replicaCount)
			continue
		}

		replicaExists[replica] = true
	}

	for replica, exists := range replicaExists {
		if !exists {
			fmt.Printf("TODO - start pod %d\n", replica)

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("flytepropeller-%d", replica),
					Namespace: m.namespace,
				},
				Spec: m.podTemplate.Template.Spec,
			}

			_, err := m.kubeClient.CoreV1().Pods(m.namespace).Create(ctx, pod, metav1.CreateOptions{})
			if err != nil {
				logger.Warnf(ctx, "failed to create replica %d - %v", replica, err)
				continue
			}
		}
	}

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.scanInterval)
	defer ticker.Stop()

	go func() {
		for {
			err := m.recoverReplicas(ctx)
			if err != nil {
				logger.Errorf(ctx, "failed to recover replicas: [%v]", err)
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
	}()

	logger.Info(ctx, "Started manager")
	<-ctx.Done()
	logger.Info(ctx, "Shutting down manager")

	return nil
}

func New(ctx context.Context, cfg *config.Config, kubeClient kubernetes.Interface) (*Manager, error) {
	podTemplate, err := kubeClient.CoreV1().PodTemplates(cfg.Namespace).Get(ctx, cfg.Template, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	manager := &Manager{
		kubeClient:   kubeClient,
		namespace:    cfg.Namespace,
		podTemplate:  podTemplate,
		replicaCount: cfg.ReplicaCount,
		scanInterval: cfg.ScanInterval.Duration,
	}

	return manager, nil
}
