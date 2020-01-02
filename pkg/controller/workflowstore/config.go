package workflowstore

import (
	config2 "github.com/lyft/flytepropeller/pkg/controller/config"
)

//go:generate pflags Config --default-var=defaultConfig

type Type string

const (
	TypeInMemory             = "InMemory"
	TypePassThrough          = "PassThrough"
	TypeResourceVersionCache = "ResourceVersionCache"
)

var (
	defaultConfig = &Config{
		Type: TypePassThrough,
	}

	configSection = config2.MustRegisterSubSection("workflowStore", defaultConfig)
)

type Config struct {
	Type                 Type    `json:"type" pflag:",Workflow Store Type to initialize"`
	ResourceVersionCache *Config `json:"resourceVersion" pflag:",Config for resource version cache store if used."`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(*cfg)
}
