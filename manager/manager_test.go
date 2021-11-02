package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreatePods(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		shardStrategy ShardStrategy
	}{
		{"hash", hashShardStrategy},
		{"hash_uncovered", hashShardStrategyUncovered},
		{"project", projectShardStrategy},
		{"project_uncovered", projectShardStrategyUncovered},
		{"domain", domainShardStrategy},
		{"domain_uncovered", domainShardStrategyUncovered},
	}

	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				v1.Container{
					Command: []string{"flytepropeller"},
					Args:    []string{"--config", "/etc/flyte/config/*.yaml"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			scope := promutils.NewScope(fmt.Sprintf("create_%s", tt.name))

			kubePodsClient := fake.NewSimpleClientset().CoreV1().Pods("")

			manager := Manager{
				kubePodsClient: kubePodsClient,
				metrics:        newManagerMetrics(scope),
				pod:            pod,
				podApplication: "flytepropeller",
				shardStrategy:  tt.shardStrategy,
			}

			// ensure no pods are "running"
			pods, err := kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(pods.Items))

			// create all pods and validate state
			err = manager.createPods(ctx)
			assert.NoError(t, err)

			pods, err = kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tt.shardStrategy.GetPodCount(), len(pods.Items))

			// execute again to ensure no new pods are created
			err = manager.createPods(ctx)
			assert.NoError(t, err)

			pods, err = kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tt.shardStrategy.GetPodCount(), len(pods.Items))
		})
	}
}

func TestDeletePods(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		shardStrategy ShardStrategy
	}{
		{"hash", hashShardStrategy},
		{"hash_uncovered", hashShardStrategyUncovered},
		{"project", projectShardStrategy},
		{"project_uncovered", projectShardStrategyUncovered},
		{"domain", domainShardStrategy},
		{"domain_uncovered", domainShardStrategyUncovered},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			scope := promutils.NewScope(fmt.Sprintf("delete_%s", tt.name))

			initPods := make([]runtime.Object, 0)
			for i := 0; i < tt.shardStrategy.GetPodCount(); i++ {
				initPods = append(initPods, &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("flytepropeller-%d", i),
					},
				})
			}

			kubePodsClient := fake.NewSimpleClientset(initPods...).CoreV1().Pods("")

			manager := Manager{
				kubePodsClient: kubePodsClient,
				metrics:        newManagerMetrics(scope),
				podApplication: "flytepropeller",
				shardStrategy:  tt.shardStrategy,
			}
			
			// ensure all pods are "running"
			pods, err := kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tt.shardStrategy.GetPodCount(), len(pods.Items))

			// delete pods and validate state
			err = manager.deletePods(ctx)
			assert.NoError(t, err)

			pods, err = kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, 0, len(pods.Items))
		})
	}
}

func TestGetPodNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		shardStrategy ShardStrategy
		podCount      int
	}{
		{"hash", hashShardStrategy, 3},
		{"hash_uncovered", hashShardStrategyUncovered, 4},
		{"project", projectShardStrategy, 2},
		{"project_uncovered", projectShardStrategyUncovered, 3},
		{"domain", domainShardStrategy, 2},
		{"domain_uncovered", domainShardStrategyUncovered, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := Manager{
				podApplication: "flytepropeller",
				shardStrategy: tt.shardStrategy,
			}

			assert.Equal(t, tt.podCount, len(manager.getPodNames()))
		})
	}
}
