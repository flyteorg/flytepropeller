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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

type Manager struct {
	kubeClient   kubernetes.Interface
	namespace    string
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
	// TODO hamersaw - move outside of function?
	re := regexp.MustCompile("^flytepropeller-([1-9]+[0-9]*)$")

	matches := re.FindAllString(str, -1)
	if matches == nil || len(matches) != 1 {
		return -1, errors.New(fmt.Sprintf("failed to parse string '%s' with regex '%s'", str, re))
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

	fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))
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
				logger.Errorf("failed to recover replicas: [%v]", err)
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
	manager := &Manager{
		kubeClient:   kubeClient,
		namespace:    cfg.Namespace,
		replicaCount: cfg.ReplicaCount,
		scanInterval: cfg.ScanInterval.Duration,
	}

	return manager, nil
}
