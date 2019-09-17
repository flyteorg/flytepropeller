package config

import (
	"github.com/lyft/flytestdlib/config"
	"k8s.io/apimachinery/pkg/util/sets"
)

//go:generate pflags Config --default-var defaultConfig

const SectionKey = "tasks"

var (
	defaultConfig = &Config{
		TaskPlugins: TaskPluginConfig{EnabledPlugins: []string{
			"container",
		}},
		MaxPluginPhaseVersions: 1000,
	}

	section = config.MustRegisterSection(SectionKey, defaultConfig)
)

type Config struct {
	TaskPlugins            TaskPluginConfig `json:"task-plugins" pflag:",Task plugin configuration"`
	MaxPluginPhaseVersions int32            `json:"max-plugin-phase-versions" pflag:",Maximum number of plugin phase versions allowed for one phase."`
}

type TaskPluginConfig struct {
	EnabledPlugins []string
}

func (p TaskPluginConfig) GetEnabledPluginsSet() sets.String {
	return sets.NewString(p.EnabledPlugins...)
}

func GetConfig() *Config {
	return section.GetConfig().(*Config)
}
