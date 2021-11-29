package shardstrategy

import (
	"fmt"

	"github.com/flyteorg/flytepropeller/pkg/utils"

	v1 "k8s.io/api/core/v1"
)

// EnvironmentShardStrategy assigns either project or domain identifers to individual
// FlytePropeller instances to determine FlyteWorkflow processing responsibility.
type EnvironmentShardStrategy struct {
	EnvType                environmentType
	EnableUncoveredReplica bool
	Replicas               [][]string
}

type environmentType int

const (
	Project environmentType = iota
	Domain
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
	container, err := utils.GetContainer(pod, containerName)
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
