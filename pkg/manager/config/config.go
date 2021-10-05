package config

import (
	"time"

	"github.com/flyteorg/flytestdlib/config"
)

//go:generate pflags Config --default-var=DefaultConfig

var (
	DefaultConfig = &Config{
		Namespace: "flyte",
		ReplicaCount: 3,
		ScanInterval: config.Duration{
			Duration: 10 * time.Second,
		},
	}

	configSection = config.MustRegisterSection("manager", DefaultConfig)
)

type Config struct {
	Namespace      string          `json:"namespace" pflag:"Namespace to use for managing flytepropeller pod instances"`
	ReplicaCount   int             `json:"replica-count" pflag:"The number of flytepropeller controller pods to manage"`
	ScanInterval   config.Duration `json:"scan-interval" pflag:"Frequency to scan flytepropeller pods and start / restart if necessary"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
