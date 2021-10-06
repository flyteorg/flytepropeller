package config

import (
	"time"

	"github.com/flyteorg/flytestdlib/config"
)

//go:generate pflags Config --default-var=DefaultConfig

var (
	DefaultConfig = &Config{
		PodApplication: "flytepropeller",
		PodNamespace: "flyte",
		PodTemplate: "flytepropeller-template",
		PodTemplateNamespace: "flyte",
		ReplicaCount: 2,
		ScanInterval: config.Duration{
			Duration: 10 * time.Second,
		},
	}

	configSection = config.MustRegisterSection("manager", DefaultConfig)
)

type Config struct {
	PodApplication       string          `json:"pod-application" pflag:"Application name for managed pods"`
	PodNamespace         string          `json:"pod-namespace" pflag:"Namespace to use for managing FlytePropeller pods"`
	PodTemplate          string          `json:"pod-template" pflag:"K8s PodTemplate name to use for starting FlytePropeller pods"`
	PodTemplateNamespace string          `json:"pod-template-namespace" pflag:"Namespace where the k8s PodTemplate is located"`
	ReplicaCount         int             `json:"replica-count" pflag:"The number of FlytePropeller controller pods to manage"`
	ScanInterval         config.Duration `json:"scan-interval" pflag:"Frequency to scan FlytePropeller pods and start / restart if necessary"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
