package manager

import (
	"context"
	"fmt"
	"testing"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flytepropeller/manager/shardstrategy"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

var (
	podTemplate = &v1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{
			ResourceVersion: "0",
		},
		Template: v1.PodTemplateSpec{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					v1.Container{
						Command: []string{"flytepropeller"},
						Args:    []string{"--config", "/etc/flyte/config/*.yaml"},
					},
				},
			},
		},
	}

	hashShardStrategy = &shardstrategy.HashShardStrategy{
		EnableUncoveredReplica: false,
		PodCount:               3,
	}

	hashShardStrategyUncovered = &shardstrategy.HashShardStrategy{
		EnableUncoveredReplica: true,
		PodCount:               3,
	}

	projectShardStrategy = &shardstrategy.EnvironmentShardStrategy{
		EnableUncoveredReplica: false,
		EnvType:                shardstrategy.Project,
		Replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	projectShardStrategyUncovered = &shardstrategy.EnvironmentShardStrategy{
		EnableUncoveredReplica: true,
		EnvType:                shardstrategy.Project,
		Replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	domainShardStrategy = &shardstrategy.EnvironmentShardStrategy{
		EnableUncoveredReplica: false,
		EnvType:                shardstrategy.Domain,
		Replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}

	domainShardStrategyUncovered = &shardstrategy.EnvironmentShardStrategy{
		EnableUncoveredReplica: true,
		EnvType:                shardstrategy.Domain,
		Replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}
)

func TestCreatePods(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		shardStrategy shardstrategy.ShardStrategy
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
			scope := promutils.NewScope(fmt.Sprintf("create_%s", tt.name))
			kubeClient := fake.NewSimpleClientset(podTemplate)

			manager := Manager{
				kubeClient:     kubeClient,
				metrics:        newManagerMetrics(scope),
				podApplication: "flytepropeller",
				shardStrategy:  tt.shardStrategy,
			}

			// ensure no pods are "running"
			kubePodsClient := kubeClient.CoreV1().Pods("")
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

func TestUpdatePods(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		shardStrategy shardstrategy.ShardStrategy
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
			scope := promutils.NewScope(fmt.Sprintf("update_%s", tt.name))

			initObjects := []runtime.Object{podTemplate}
			for i := 0; i < tt.shardStrategy.GetPodCount(); i++ {
				initObjects = append(initObjects, &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							podTemplateResourceVersion: "1",
							shardConfigHash:            "1",
						},
						Labels: map[string]string{
							"app": "flytepropeller",
						},
						Name: fmt.Sprintf("flytepropeller-%d", i),
					},
				})
			}

			kubeClient := fake.NewSimpleClientset(initObjects...)

			manager := Manager{
				kubeClient:     kubeClient,
				metrics:        newManagerMetrics(scope),
				podApplication: "flytepropeller",
				shardStrategy:  tt.shardStrategy,
			}

			// ensure all pods are "running"
			kubePodsClient := kubeClient.CoreV1().Pods("")
			pods, err := kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tt.shardStrategy.GetPodCount(), len(pods.Items))
			for _, pod := range pods.Items {
				assert.Equal(t, "1", pod.ObjectMeta.Annotations[podTemplateResourceVersion])
			}

			// create all pods and validate state
			err = manager.createPods(ctx)
			assert.NoError(t, err)

			pods, err = kubePodsClient.List(ctx, metav1.ListOptions{})
			assert.NoError(t, err)
			assert.Equal(t, tt.shardStrategy.GetPodCount(), len(pods.Items))
			for _, pod := range pods.Items {
				assert.Equal(t, podTemplate.ObjectMeta.ResourceVersion, pod.ObjectMeta.Annotations[podTemplateResourceVersion])
			}
		})
	}
}

func TestGetPodNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		shardStrategy shardstrategy.ShardStrategy
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
				shardStrategy:  tt.shardStrategy,
			}

			assert.Equal(t, tt.podCount, len(manager.getPodNames()))
		})
	}
}
