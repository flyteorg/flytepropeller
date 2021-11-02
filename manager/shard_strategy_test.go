package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
)

var (
	hashShardStrategy = &HashShardStrategy{
		enableUncoveredReplica: false,
		podCount:               3,
	}

	hashShardStrategyUncovered = &HashShardStrategy{
		enableUncoveredReplica: true,
		podCount:               3,
	}

	projectShardStrategy = &EnvironmentShardStrategy{
		enableUncoveredReplica: false,
		envType:                project,
		replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	projectShardStrategyUncovered = &EnvironmentShardStrategy{
		enableUncoveredReplica: true,
		envType:                project,
		replicas: [][]string{
			[]string{"flytesnacks"},
			[]string{"flytefoo", "flytebar"},
		},
	}

	domainShardStrategy = &EnvironmentShardStrategy{
		enableUncoveredReplica: false,
		envType:                domain,
		replicas: [][]string{
			[]string{"production"},
			[]string{"foo", "bar"},
		},
	}

	domainShardStrategyUncovered = &EnvironmentShardStrategy{
		enableUncoveredReplica: true,
		envType:                domain,
		replicas: [][]string{
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
			startIndex, endIndex := computeKeyRange(keyspaceSize, podCount, podIndex)

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
		{"hash-uncovered", hashShardStrategyUncovered, 4},
		{"project", projectShardStrategy, 2},
		{"project-uncovered", projectShardStrategyUncovered, 3},
		{"domain", domainShardStrategy, 2},
		{"domain-uncovered", domainShardStrategyUncovered, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.podCount, tt.shardStrategy.GetPodCount())
		})
	}
}

func TestUpdatePodSpec(t *testing.T) {
	tests := []struct {
		name          string
		shardStrategy ShardStrategy
	}{
		{"hash", hashShardStrategy},
		{"hash-uncovered", hashShardStrategyUncovered},
		{"project", projectShardStrategy},
		{"project-uncovered", projectShardStrategyUncovered},
		{"domain", domainShardStrategy},
		{"domain-uncovered", domainShardStrategyUncovered},
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

				// TODO hamersaw - validate podSpec.Args
			}
		})
	}
}

func TestUpdatePodSpecInvalidPodIndex(t *testing.T) {
	tests := []struct {
		name          string
		shardStrategy ShardStrategy
	}{
		{"hash", hashShardStrategy},
		{"hash-uncovered", hashShardStrategyUncovered},
		{"project", projectShardStrategy},
		{"project-uncovered", projectShardStrategyUncovered},
		{"domain", domainShardStrategy},
		{"domain-uncovered", domainShardStrategyUncovered},
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

			err := tt.shardStrategy.UpdatePodSpec(&podSpec, -1)
			assert.Error(t, err)

			err = tt.shardStrategy.UpdatePodSpec(&podSpec, tt.shardStrategy.GetPodCount()+1)
			assert.Error(t, err)
		})
	}
}

func TestUpdatePodSpecInvalidPodSpec(t *testing.T) {
	tests := []struct {
		name          string
		shardStrategy ShardStrategy
	}{
		{"hash", hashShardStrategy},
		{"hash-uncovered", hashShardStrategyUncovered},
		{"project", projectShardStrategy},
		{"project-uncovered", projectShardStrategyUncovered},
		{"domain", domainShardStrategy},
		{"domain-uncovered", domainShardStrategyUncovered},
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
