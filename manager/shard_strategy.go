package manager

import (
	"context"
	"fmt"

	"github.com/flyteorg/flytepropeller/manager/config"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	v1 "k8s.io/api/core/v1"
)

// Defines necessary functionality for a sharding strategy
type ShardStrategy interface {
	// Returns the total number of pods for the sharding strategy
	GetPodCount() int
	// Updates the PodSpec for the specified index to include label selectors
	UpdatePodSpec(pod *v1.PodSpec, podIndex int) error
}

// Creates and validates a new ShardStrategy defined by the configuration
func NewShardStrategy(ctx context.Context, shardConfig config.ShardConfig) (ShardStrategy, error) {
	switch shardConfig.Type {
	case config.HashShardType:
		if shardConfig.PodCount <= 0 {
			return nil, fmt.Errorf("configured PodCount (%d) must be greater than zero", shardConfig.PodCount)
		} else if shardConfig.PodCount > v1alpha1.ShardKeyspaceSize {
			return nil, fmt.Errorf("configured PodCount (%d) is larger than available keyspace size (%d)", shardConfig.PodCount, v1alpha1.ShardKeyspaceSize)
		}

		return &HashShardStrategy{
			enableUncoveredReplica: shardConfig.EnableUncoveredReplica,
			podCount:               shardConfig.PodCount,
		}, nil
	case config.ProjectShardType, config.DomainShardType:
		replicas := make([][]string, 0)
		for _, replica := range shardConfig.Replicas {
			if len(replica.Entities) == 0 {
				return nil, fmt.Errorf("unable to create replica with 0 configured entity(ies)")
			}

			replicas = append(replicas, replica.Entities)
		}

		var envType environmentType
		switch shardConfig.Type {
		case config.ProjectShardType:
			envType = project
		case config.DomainShardType:
			envType = domain
		}

		return &EnvironmentShardStrategy{
			enableUncoveredReplica: shardConfig.EnableUncoveredReplica,
			envType:                envType,
			replicas:               replicas,
		}, nil
	}

	return nil, fmt.Errorf("shard strategy '%s' does not exist", shardConfig.Type)
}

// HashShardStrategy evenly assigns disjoint keyspace responsibilities over a collection of pods.
// All FlyteWorkflows are assigned a shard-key using a hash of their executionID and are then
// processed by the FlytePropeller instance responsible for that keyspace range.
type HashShardStrategy struct {
	enableUncoveredReplica bool
	podCount               int
}

func (h *HashShardStrategy) GetPodCount() int {
	if h.enableUncoveredReplica {
		return h.podCount + 1
	}

	return h.podCount
}

func (h *HashShardStrategy) UpdatePodSpec(pod *v1.PodSpec, podIndex int) error {
	container, err := getFlytePropellerContainer(pod)
	if err != nil {
		return err
	}

	if podIndex >= 0 && podIndex < h.podCount {
		startKey, endKey := ComputeKeyRange(v1alpha1.ShardKeyspaceSize, h.podCount, podIndex)
		for i := startKey; i < endKey; i++ {
			container.Args = append(container.Args, "--propeller.include-shard-label", fmt.Sprintf("%d", i))
		}
	} else if h.enableUncoveredReplica && podIndex == h.podCount {
		for i := 0; i < v1alpha1.ShardKeyspaceSize; i++ {
			container.Args = append(container.Args, "--propeller.exclude-shard-label", fmt.Sprintf("%d", i))
		}
	} else {
		return fmt.Errorf("invalid podIndex '%d' out of range [0,%d)", podIndex, h.GetPodCount())
	}

	return nil
}

// Computes a [startKey, endKey) pair denoting the key responsibilities for the provided pod index
// given the keyspaceSize and podCount parameters
func ComputeKeyRange(keyspaceSize, podCount, podIndex int) (int, int) {
	keysPerPod := keyspaceSize / podCount
	keyRemainder := keyspaceSize - (podCount * keysPerPod)

	return computeStartKey(keysPerPod, keyRemainder, podIndex), computeStartKey(keysPerPod, keyRemainder, podIndex+1)
}

func computeStartKey(keysPerPod, keysRemainder, podIndex int) int {
	return (intMin(podIndex, keysRemainder) * (keysPerPod + 1)) + (intMax(0, podIndex-keysRemainder) * keysPerPod)
}

func intMin(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func intMax(a, b int) int {
	if a > b {
		return a
	}

	return b
}

// The ProjectShardStrategy assigns project(s) to individual FlytePropeller instances to
// determine FlyteWorkflow processing responsibility.
type EnvironmentShardStrategy struct {
	envType                environmentType
	enableUncoveredReplica bool
	replicas               [][]string
}

type environmentType int

const (
	project environmentType = iota
	domain
)

func (e environmentType) String() string {
	return [...]string{"project", "domain"}[e]
}

func (e *EnvironmentShardStrategy) GetPodCount() int {
	if e.enableUncoveredReplica {
		return len(e.replicas) + 1
	}

	return len(e.replicas)
}

func (e *EnvironmentShardStrategy) UpdatePodSpec(pod *v1.PodSpec, podIndex int) error {
	container, err := getFlytePropellerContainer(pod)
	if err != nil {
		return err
	}

	if podIndex >= 0 && podIndex < len(e.replicas) {
		for _, entity := range e.replicas[podIndex] {
			container.Args = append(container.Args, fmt.Sprintf("--propeller.include-%s-label", e.envType), entity)
		}
	} else if e.enableUncoveredReplica && podIndex == len(e.replicas) {
		for _, replica := range e.replicas {
			for _, entity := range replica {
				container.Args = append(container.Args, fmt.Sprintf("--propeller.exclude-%s-label", e.envType), entity)
			}
		}
	} else {
		return fmt.Errorf("invalid podIndex '%d' out of range [0,%d)", podIndex, e.GetPodCount())
	}

	return nil
}

func getFlytePropellerContainer(pod *v1.PodSpec) (*v1.Container, error) {
	// find flytepropeller container(s)
	var containers []*v1.Container
	for i := 0; i < len(pod.Containers); i++ {
		commands := pod.Containers[i].Command
		if len(commands) > 0 && commands[0] == "flytepropeller" {
			containers = append(containers, &pod.Containers[i])
		}
	}

	if len(containers) != 1 {
		return nil, fmt.Errorf("expecting 1 flytepropeller container in podtemplate but found %d, ", len(containers))
	}

	return containers[0], nil
}
