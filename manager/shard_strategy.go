package manager

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"hash/fnv"

	"github.com/flyteorg/flytepropeller/manager/config"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	v1 "k8s.io/api/core/v1"
)

// Defines necessary functionality for a sharding strategy
type ShardStrategy interface {
	// Returns the total number of pods for the sharding strategy
	GetPodCount() int
	// Generates a unique hash code to identify shard strategy updates
	HashCode() (uint32, error)
	// Updates the PodSpec for the specified index to include label selectors
	UpdatePodSpec(pod *v1.PodSpec, containerName string, podIndex int) error
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
			EnableUncoveredReplica: shardConfig.EnableUncoveredReplica,
			PodCount:               shardConfig.PodCount,
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
			EnableUncoveredReplica: shardConfig.EnableUncoveredReplica,
			EnvType:                envType,
			Replicas:               replicas,
		}, nil
	}

	return nil, fmt.Errorf("shard strategy '%s' does not exist", shardConfig.Type)
}

// HashShardStrategy evenly assigns disjoint keyspace responsibilities over a collection of pods.
// All FlyteWorkflows are assigned a shard-key using a hash of their executionID and are then
// processed by the FlytePropeller instance responsible for that keyspace range.
type HashShardStrategy struct {
	EnableUncoveredReplica bool
	PodCount               int
}

func (h *HashShardStrategy) GetPodCount() int {
	if h.EnableUncoveredReplica {
		return h.PodCount + 1
	}

	return h.PodCount
}

func (h *HashShardStrategy) HashCode() (uint32, error) {
	return computeHashCode(config.HashShardType, h)
}

func (h *HashShardStrategy) UpdatePodSpec(pod *v1.PodSpec, containerName string, podIndex int) error {
	container, err := getContainer(pod, containerName)
	if err != nil {
		return err
	}

	if podIndex >= 0 && podIndex < h.PodCount {
		startKey, endKey := ComputeKeyRange(v1alpha1.ShardKeyspaceSize, h.PodCount, podIndex)
		for i := startKey; i < endKey; i++ {
			container.Args = append(container.Args, "--propeller.include-shard-key-label", fmt.Sprintf("%d", i))
		}
	} else if h.EnableUncoveredReplica && podIndex == h.PodCount {
		for i := 0; i < v1alpha1.ShardKeyspaceSize; i++ {
			container.Args = append(container.Args, "--propeller.exclude-shard-key-label", fmt.Sprintf("%d", i))
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
	EnvType                environmentType
	EnableUncoveredReplica bool
	Replicas               [][]string
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
	if e.EnableUncoveredReplica {
		return len(e.Replicas) + 1
	}

	return len(e.Replicas)
}

func (e *EnvironmentShardStrategy) HashCode() (uint32, error) {
	return computeHashCode(e.EnvType.String(), e)
}

func (e *EnvironmentShardStrategy) UpdatePodSpec(pod *v1.PodSpec, containerName string, podIndex int) error {
	container, err := getContainer(pod, containerName)
	if err != nil {
		return err
	}

	if podIndex >= 0 && podIndex < len(e.Replicas) {
		for _, entity := range e.Replicas[podIndex] {
			container.Args = append(container.Args, fmt.Sprintf("--propeller.include-%s-label", e.EnvType), entity)
		}
	} else if e.EnableUncoveredReplica && podIndex == len(e.Replicas) {
		for _, replica := range e.Replicas {
			for _, entity := range replica {
				container.Args = append(container.Args, fmt.Sprintf("--propeller.exclude-%s-label", e.EnvType), entity)
			}
		}
	} else {
		return fmt.Errorf("invalid podIndex '%d' out of range [0,%d)", podIndex, e.GetPodCount())
	}

	return nil
}

func getContainer(pod *v1.PodSpec, containerName string) (*v1.Container, error) {
	// find flytepropeller container(s)
	var containers []*v1.Container
	for i := 0; i < len(pod.Containers); i++ {
		if pod.Containers[i].Name == containerName {
			containers = append(containers, &pod.Containers[i])
		}
	}

	if len(containers) != 1 {
		return nil, fmt.Errorf("expecting 1 flytepropeller container in podtemplate but found %d, ", len(containers))
	}

	return containers[0], nil
}

func computeHashCode(name string, data interface{}) (uint32, error) {
	hash := fnv.New32a()
	if _, err := hash.Write([]byte(name)); err != nil {
		return 0, err
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(data); err != nil {
		return 0, err
	}

	if _, err := hash.Write(buffer.Bytes()); err != nil {
		return 0, err
	}

	return hash.Sum32(), nil
}
