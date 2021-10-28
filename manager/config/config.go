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
		PodTemplateName:      "flytepropeller-template",
		PodTemplateNamespace: "flyte",
		ScanInterval: config.Duration{
			Duration: 10 * time.Second,
		},
		ShardConfig: ShardConfig{
			Type:                   "random",
			EnableUncoveredReplica: false,
			PodCount:               3,
		},
	}

	configSection = config.MustRegisterSection("manager", DefaultConfig)
)

type ShardType = string

const (
	NamespaceShardType ShardType = "namespace"
	RandomShardType    ShardType = "random"
)

type ReplicaConfig struct {
	Namespaces []string `json:"namespaces" pflag:",The list of namespaces to be managed"`
}

type ShardConfig struct {
	Type                   ShardType       `json:"type" pflag:"\"random\",Shard implementation to use"`
	EnableUncoveredReplica bool            `json:"enable-uncovered-replica" pflag:"\"true\",Start a replica to process FlyteWorkflows that are uncovered by the configured shard label selectors"`
	NamespaceReplicas      []ReplicaConfig `json:"replicas" pflag:"-"`
	PodCount               int             `json:"pod-count" pflag:"\"3\",The number of pods to manage for a random shard type"`
}

type Config struct {
	PodApplication       string          `json:"pod-application" pflag:"\"flytepropeller\",Application name for managed pods"`
	PodNamespace         string          `json:"pod-namespace" pflag:"\"flyte\",Namespace to use for managing FlytePropeller pods"`
	PodTemplateName      string          `json:"pod-template-name" pflag:"\"flytepropeller-template\",K8s PodTemplate name to use for starting FlytePropeller pods"`
	PodTemplateNamespace string          `json:"pod-template-namespace" pflag:"\"flyte\",Namespace where the k8s PodTemplate is located"`
	ScanInterval         config.Duration `json:"scan-interval" pflag:"\"10s\",Frequency to scan FlytePropeller pods and start / restart if necessary"`
	ShardConfig          ShardConfig     `json:"shard" pflag:",Configure the shard strategy for this manager"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
