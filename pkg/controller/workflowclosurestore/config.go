package workflowclosurestore

import (
	ctrlConfig "github.com/flyteorg/flytepropeller/pkg/controller/config"
)

//go:generate pflags Config --default-var=defaultConfig

type Policy = string

const (
	PolicyActive      = "Active"
	PolicyLRU         = "LRU"
	PolicyPassThrough = "PassThrough"
)

var (
	defaultConfig = &Config{
		Policy: PolicyLRU,
		Size:   1000,
	}

	configSection = ctrlConfig.MustRegisterSubSection("wfClosureStore", defaultConfig)
)

type Config struct {
	Policy Policy `json:"policy" pflag:",WorkflowClosure Offload Store Policy to initialize"`
	Size   int    `json:"size" pflag:",The maximum size of the LRU cache"`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(*cfg)
}
