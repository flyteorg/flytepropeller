package manager

import (
	"context"
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

func (m *Manager) amendReplicas(ctx context.Context) error {
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

	re, err := regexp.Compile("flytepropeller-([1-9]+[0-9]*)")
	if err != nil {
		fmt.Println("failed to compile regex")
		return nil
	}

	//  find missing pod and start
	for _, pod := range pods.Items {
		matches := re.FindAllString(pod.ObjectMeta.Name, -1)
		if matches == nil || len(matches) != 1 {
			fmt.Println("invalid replica pod name - regex")
			// TODO hamersaw - invalid replica pod name
			continue
		}

		for _, match := range matches {
			replica, err := strconv.ParseInt(match, 0, 32)
			if err != nil {
				fmt.Println("invalid replica pod name - parse")
				// TODO hamersaw - invalid replica pod name
				continue
			}

			intReplica := int(replica)
			if intReplica < 0 || intReplica >= m.replicaCount {
				fmt.Println("invalid replica pod name - range")
				// TODO hamersaw - out of range
				continue
			}

			replicaExists[intReplica] = true
		}
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
			err := m.amendReplicas(ctx)
			if err != nil {
				// TODO hamersaw - log?
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
