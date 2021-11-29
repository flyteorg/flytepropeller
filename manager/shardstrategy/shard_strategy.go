package shardstrategy

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

//go:generate mockery -name ShardStrategy -case=underscore

// ShardStrategy defines necessary functionality for a sharding strategy
type ShardStrategy interface {
	// Returns the total number of pods for the sharding strategy
	GetPodCount() int
	// Generates a unique hash code to identify shard strategy updates
	HashCode() (uint32, error)
	// Updates the PodSpec for the specified index to include label selectors
	UpdatePodSpec(pod *v1.PodSpec, containerName string, podIndex int) error
}

// NewShardStrategy creates and validates a new ShardStrategy defined by the configuration
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
			envType = Project
		case config.DomainShardType:
			envType = Domain
		}

		return &EnvironmentShardStrategy{
			EnableUncoveredReplica: shardConfig.EnableUncoveredReplica,
			EnvType:                envType,
			Replicas:               replicas,
		}, nil
	}

	return nil, fmt.Errorf("shard strategy '%s' does not exist", shardConfig.Type)
}

func computeHashCode(name string, data interface{}) (uint32, error) {
	hash := fnv.New32a()
	if _, err := hash.Write([]byte(name)); err != nil {
		return 0, err
	}

	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	if err := encoder.Encode(data); err != nil {
		return 0, err
	}

	if _, err := hash.Write(buffer.Bytes()); err != nil {
		return 0, err
	}

	return hash.Sum32(), nil
}
