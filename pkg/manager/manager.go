package manager

import (
	"context"
	"fmt"
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
	scanInterval time.Duration

	// Kubernetes API.
	//metrics       *metrics
}

/*type metrics struct {
	Scope            promutils.Scope
	EnqueueCountWf   prometheus.Counter
	EnqueueCountTask prometheus.Counter
}*/

func (m *Manager) Run(ctx context.Context) error {
	// TODO hamersaw - use [ListOptions](https://pkg.go.dev/k8s.io/apimachinery@v0.20.2/pkg/apis/meta/v1#ListOptions)
	labelMap := map[string]string{
		"app": "flytepropeller",
	}
	options := metav1.ListOptions{
        LabelSelector: labels.SelectorFromSet(labelMap).String(),
    }

	ticker := time.NewTicker(m.scanInterval)
	defer ticker.Stop()

	go func() {
		for {
			pods, err := m.kubeClient.CoreV1().Pods(m.namespace).List(ctx, options)
			if err != nil {
				panic(err.Error())
			}
			fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

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
		scanInterval: cfg.ScanInterval.Duration,
	}

	return manager, nil
}
