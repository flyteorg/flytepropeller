package manager

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
		EnvType:                project,
		Replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	projectShardStrategyUncovered = &EnvironmentShardStrategy{
		EnableUncoveredReplica: true,
		EnvType:                project,
		Replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	domainShardStrategy = &EnvironmentShardStrategy{
		EnableUncoveredReplica: false,
		EnvType:                domain,
		Replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}

	domainShardStrategyUncovered = &EnvironmentShardStrategy{
		EnableUncoveredReplica: true,
		EnvType:                domain,
		Replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}
)

func TestComputeKeyRange(t *testing.T) {
	keyspaceSize := 32
	for podCount := 1; podCount < keyspaceSize; podCount++ {
		keysCovered := 0
		minKeyRangeSize := keyspaceSize / podCount
		for podIndex := 0; podIndex < podCount; podIndex++ {
			startIndex, endIndex := ComputeKeyRange(keyspaceSize, podCount, podIndex)

			rangeSize := endIndex - startIndex
			keysCovered += rangeSize
			assert.True(t, rangeSize-minKeyRangeSize >= 0)
			assert.True(t, rangeSize-minKeyRangeSize <= 1)
		}

		assert.Equal(t, keyspaceSize, keysCovered)
	}
}

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
							Command: []string{"flytepropeller"},
							Args:    []string{"--config", "/etc/flyte/config/*.yaml"},
						},
					},
				}

				err := tt.shardStrategy.UpdatePodSpec(&podSpec, podIndex)
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
						Command: []string{"flytepropeller"},
						Args:    []string{"--config", "/etc/flyte/config/*.yaml"},
					},
				},
			}

			lowerErr := tt.shardStrategy.UpdatePodSpec(&podSpec, -1)
			assert.Error(t, lowerErr)

			upperErr := tt.shardStrategy.UpdatePodSpec(&podSpec, tt.shardStrategy.GetPodCount())
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
						Command: []string{"flytefoo"},
						Args:    []string{"--config", "/etc/flyte/config/*.yaml"},
					},
				},
			}

			err := tt.shardStrategy.UpdatePodSpec(&podSpec, 0)
			assert.Error(t, err)
		})
	}
}
