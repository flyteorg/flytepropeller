package config

import (
	"time"

	"github.com/flyteorg/flytestdlib/config"
)

//go:generate pflags Config --default-var=DefaultConfig

var (
	DefaultConfig = &Config{
		PodApplication:       "flytepropeller",
		PodNamespace:         "flyte",
		PodTemplate:          "flytepropeller-template",
		PodTemplateNamespace: "flyte",
		ScanInterval: config.Duration{
			Duration: 10 * time.Second,
		},
		ShardConfig: ShardConfig{
			Type: "consistent-hashing",
			PodCount: 3,
			KeyspaceSize: 32,
		},
	}

	configSection = config.MustRegisterSection("manager", DefaultConfig)
)

type ShardType = string

const (
	ConsistentHashingShardType ShardType = "consistent-hashing"
	//ProjectShardType           ShardType = "project" // TODO hamersaw - implement
)

type ShardConfig struct {
	Type         ShardType `json:"type" pflag:",Shard implementation to use"`
	PodCount     int       `json:"pod-count" pflag:",The number of pods to manage for consistent hashing"`
	KeyspaceSize int       `json:"keyspace-size" pflag:",Size of the keyspace to use in consistent hashing"`
}

type Config struct {
	PodApplication       string          `json:"pod-application" pflag:"\"flytepropeller\",Application name for managed pods"`
	PodNamespace         string          `json:"pod-namespace" pflag:"\"flyte\",Namespace to use for managing FlytePropeller pods"`
	PodTemplate          string          `json:"pod-template" pflag:"\"flytepropeller-template\",K8s PodTemplate name to use for starting FlytePropeller pods"`
	PodTemplateNamespace string          `json:"pod-template-namespace" pflag:"\"flyte\",Namespace where the k8s PodTemplate is located"`
	ScanInterval         config.Duration `json:"scan-interval" pflag:"\"5s\",Frequency to scan FlytePropeller pods and start / restart if necessary"`
	ShardConfig          ShardConfig     `json:"shard" pflag:",Configure the shard strategy for this manager"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
