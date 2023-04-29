package config

import (
	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config

const AppName = "flyteSchedulerExtender"
const configSectionKey = "flyteSchedulerExtender"

var ConfigSection = config.MustRegisterSection(configSectionKey, &Config{})

type Config struct {
	ResyncPeriod  config.Duration `json:"resyncPeriod" pflag:"\"30s\",Determines the resync period for all watchers."`
	MetricsPrefix string          `json:"metricsPrefix" pflag:"\"flyteK8sSchedulerExtension\",Prefix for metrics propagated to prometheus"`
	ProfilerPort  config.Port     `json:"profilerPort" pflag:"\"10254\",Profiler port"`
	Workers int `json:"workers" pflag:"4,Number of routines to process custom resource"`

	UnrestrictedCPUAutoscalingLimit    int `json:"unrestrictedCpuAutoscalingLimit" pflag:"30,Max number of cpus for unrestricted auto-scaling."`
	UnrestrictedMemoryAutoscalingLimit int `json:"unrestrictedMemoryAutoscalingLimit" pflag:"30,Max amount of memory for unrestricted auto-scaling."`
}

func GetConfig() *Config {
	return ConfigSection.GetConfig().(*Config)
}

func SetConfig(c *Config) error {
	return ConfigSection.SetConfig(c)
}
