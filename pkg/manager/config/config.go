package config

import (
	"github.com/flyteorg/flytestdlib/config"
)

//go:generate pflags Config --default-var=DefaultConfig

var (
	DefaultConfig = &Config{
		TestStr: "hello world",
	}

	configSection = config.MustRegisterSection("manager", DefaultConfig)
)

type Config struct {
	TestStr string `json:"test-str" pflag:",A string to test the command is working"`
	// TODO - flytepropeller namespace (default: "flyte")
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
