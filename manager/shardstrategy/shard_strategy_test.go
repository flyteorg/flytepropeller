package shardstrategy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
)

var (
	hashShardStrategy = &HashShardStrategy{
		EnableUncoveredReplica: false,
		PodCount:               3,
	}

	hashShardStrategyUncovered = &HashShardStrategy{
		EnableUncoveredReplica: true,
		PodCount:               3,
	}

	projectShardStrategy = &EnvironmentShardStrategy{
		EnableUncoveredReplica: false,
		EnvType:                Project,
		Replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	projectShardStrategyUncovered = &EnvironmentShardStrategy{
		EnableUncoveredReplica: true,
		EnvType:                Project,
		Replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	domainShardStrategy = &EnvironmentShardStrategy{
		EnableUncoveredReplica: false,
		EnvType:                Domain,
		Replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}

	domainShardStrategyUncovered = &EnvironmentShardStrategy{
		EnableUncoveredReplica: true,
		EnvType:                Domain,
		Replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}
)

func TestGetPodCount(t *testing.T) {
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
			assert.Equal(t, tt.podCount, tt.shardStrategy.GetPodCount())
		})
	}
}

func TestUpdatePodSpec(t *testing.T) {
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
			for podIndex := 0; podIndex < tt.shardStrategy.GetPodCount(); podIndex++ {
				podSpec := v1.PodSpec{
					Containers: []v1.Container{
						v1.Container{
							Name: "flytepropeller",
						},
					},
				}

				err := tt.shardStrategy.UpdatePodSpec(&podSpec, "flytepropeller", podIndex)
				assert.NoError(t, err)
			}
		})
	}
}

func TestUpdatePodSpecInvalidPodIndex(t *testing.T) {
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
			podSpec := v1.PodSpec{
				Containers: []v1.Container{
					v1.Container{
						Name: "flytepropeller",
					},
				},
			}

			lowerErr := tt.shardStrategy.UpdatePodSpec(&podSpec, "flytepropeller", -1)
			assert.Error(t, lowerErr)

			upperErr := tt.shardStrategy.UpdatePodSpec(&podSpec, "flytepropeller", tt.shardStrategy.GetPodCount())
			assert.Error(t, upperErr)
		})
	}
}

func TestUpdatePodSpecInvalidPodSpec(t *testing.T) {
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
			podSpec := v1.PodSpec{
				Containers: []v1.Container{
					v1.Container{
						Name: "flytefoo",
					},
				},
			}

			err := tt.shardStrategy.UpdatePodSpec(&podSpec, "flytepropeller", 0)
			assert.Error(t, err)
		})
	}
}
