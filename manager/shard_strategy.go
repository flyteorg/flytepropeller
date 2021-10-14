package manager

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/flyteorg/flytepropeller/manager/config"

	v1 "k8s.io/api/core/v1"
)

func updatePod(pod *v1.Pod) {
	pod.ObjectMeta.Name = "foobar"
}

type ShardStrategy interface {
	GetPodCount() (int, error)
	UpdatePodSpec(pod *v1.PodSpec, podIndex int) error
}

func NewShardStrategy(ctx context.Context, shardConfig config.ShardConfig) (ShardStrategy, error) {
	switch shardConfig.Type {
	case config.ConsistentHashingShardType:
		return &ConsistentHashingShardStrategy{
			podCount: shardConfig.PodCount,
			keyspaceSize: shardConfig.KeyspaceSize,
		}, nil
	}

	return nil, fmt.Errorf("shard strategy '%s' does not exist", shardConfig.Type)
}

type ConsistentHashingShardStrategy struct {
	podCount int
	keyspaceSize int
}

func (c *ConsistentHashingShardStrategy) GetPodCount() (int, error) {
	return c.podCount, nil
}

func (c *ConsistentHashingShardStrategy) UpdatePodSpec(pod *v1.PodSpec, podIndex int) error {
	container, err := getFlytePropellerContainer(pod)
	if err != nil {
		return err
	}

	startKey, endKey := computeKeyRange(c.keyspaceSize, c.podCount, podIndex)
	for i := startKey; i < endKey; i++ {
		container.Args = append(container.Args, "--propeller.include-shard-key", fmt.Sprintf("%d", i))
	}

	return nil
}

// computes a [startKey, endKey) pair denoting the key responsibilities for the provided pod index
// given the keyspaceSize and podCount parameters
func computeKeyRange(keyspaceSize, podCount, podIndex int) (int, int) {
	// TODO hamersaw - validate podCount != 0 and rest of parameters
	keysPerPod := int(math.Floor(float64(keyspaceSize / podCount)))
	keyRemainder := keyspaceSize % keysPerPod

	return computeStartKey(keysPerPod, keyRemainder, podIndex), computeStartKey(keysPerPod, keyRemainder, podIndex+1)
}

func computeStartKey(keysPerPod, keysRemainder, podIndex int) int {
	return (intMin(podIndex, keysRemainder) * (keysPerPod + 1)) + (intMax(0, podIndex - keysRemainder) * keysPerPod)
}

func intMin(a, b int) int {
	if a < b{
		return a
	}

	return b
}

func intMax(a, b int) int {
	if a > b{
		return a
	}

	return b
}

/*
TODO hamersaw - implement LoadBalancingShardStrategy

add "--report-metrics=id" statistics to flytepropeller command

report metrics does two things
(1) uses label selectors on FlyteWorkflow CRD for the specified id
(2) launches a periodic metric report of workflow(s) / node(s) / task(s)

flyteadmin uses the metric reports to label FlyteWorkflow CRDs based on usage
*/
type LoadBalancingShardStrategy struct {
}

func (l *LoadBalancingShardStrategy) GetPodCount() (int, error) {
	return -1, errors.New("unimplemented")
}

func (l *LoadBalancingShardStrategy) UpdatePodSpec(pod *v1.PodSpec, podIndex int) error {
	return errors.New("unimplemented")
}

/*
TODO hamersaw - implement ProjectDomainShardStrategy

project domain shard strategy configuration includes a list of lists of <project, domain> KV pairs. for example:
strategy:
- node:
  - pair:
    project: flytesnacks
    domain: development
  - pair:
    project: flytesnacks
    domain: staging
- node:
  - pair:
    project: flytefoo

this strategy starts len(node) + 1 nodes where each node is responsible for the project:domain hashes (with a large hashspace - 64bits?) defined and one more for everything else. nodes for the previous example:

node 0 "token in [4, 8]" // one hash for each KV pair
node 1 "token in [3, 7, 12]" // for staging, development, production domains
node 2 "token not in [4, 8, 3, 7, 12]" // handle all unhandled CRDs
*/
type ProjectDomainShardStrategy struct {
}

func (l *ProjectDomainShardStrategy) GetPodCount() (int, error) {
	return -1, errors.New("unimplemented")
}

func (p *ProjectDomainShardStrategy) UpdatePodSpec(pod *v1.PodSpec, podIndex int) error {
	return errors.New("unimplemented")
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
		return nil, errors.New(fmt.Sprintf("expecting 1 flytepropeller container in podtemplate but found %d, ", len(containers)))
	}

	return containers[0], nil
}

