package config

import (
	"github.com/lyft/flytepropeller/pkg/controller/config"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager"
)

// go:generate pflags Config

const resourceManagerConfigSectionKey = "resourcemanager"

var (
	defaultConfig = Config{
		ResourceManagerType:     resourcemanager.ResourceManagerTypeNoop,
		// ResourceQuota:           200,
	}

	resourceManagerConfigSection = config.MustRegisterSubSection(resourceManagerConfigSectionKey, &defaultConfig)
)

type Config struct {
	ResourceManagerType    resourcemanager.ResourceManagerType   `json:"resourceManagerType" pflag:"noop,Which resource manager to use"`
	// ResourceQuota          int			   `json:"resourceQuota" pflag:",Global limit for concurrent Qubole queries"`
	RedisHostPath          string          `json:"redisHostPath" pflag:",Redis host location"`
	RedisHostKey           string          `json:"redisHostKey" pflag:",Key for local Redis access"`
	RedisMaxRetries        int             `json:"redisMaxRetries" pflag:",See Redis client options for more info"`
}

// Retrieves the current config value or default.
func GetResourceManagerConfig() *Config {
	return resourceManagerConfigSection.GetConfig().(*Config)
}

func SetResourceManagerConfig(cfg *Config) error {
	return resourceManagerConfigSection.SetConfig(cfg)
}