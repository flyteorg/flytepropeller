package config

import (
	"strings"
	"time"

	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config --default-var defaultConfig

const SectionKey = "tasks"

var (
	defaultConfig = &Config{
		TaskPlugins:            TaskPluginConfig{EnabledPlugins: []string{}, DefaultForTaskTypes: map[string]string{}},
		MaxPluginPhaseVersions: 100000,
		BarrierConfig: BarrierConfig{
			Enabled:   true,
			CacheSize: 10000,
			CacheTTL:  config.Duration{Duration: time.Minute * 30},
		},
		BackOffConfig: BackOffConfig{
			BaseSecond:  2,
			MaxDuration: config.Duration{Duration: time.Minute * 10},
		},
		MaxErrorMessageLength: 2048,
	}

	section = config.MustRegisterSection(SectionKey, defaultConfig)
)

type Config struct {
	TaskPlugins            TaskPluginConfig `json:"task-plugins" pflag:",Task plugin configuration"`
	MaxPluginPhaseVersions int32            `json:"max-plugin-phase-versions" pflag:",Maximum number of plugin phase versions allowed for one phase."`
	BarrierConfig          BarrierConfig    `json:"barrier" pflag:",Config for Barrier implementation"`
	BackOffConfig          BackOffConfig    `json:"backoff" pflag:",Config for Exponential BackOff implementation"`
	MaxErrorMessageLength  int              `json:"maxLogMessageLength" pflag:",Max length of error message."`
}

type BarrierConfig struct {
	Enabled   bool            `json:"enabled" pflag:",Enable Barrier transitions using inmemory context"`
	CacheSize int             `json:"cache-size" pflag:",Max number of barrier to preserve in memory"`
	CacheTTL  config.Duration `json:"cache-ttl" pflag:", Max duration that a barrier would be respected if the process is not restarted. This should account for time required to store the record into persistent storage (across multiple rounds."`
}

type TaskPluginConfig struct {
	EnabledPlugins []string `json:"enabled-plugins" pflag:",deprecated"`
	// Maps task types to their plugin handler (by ID).
	DefaultForTaskTypes map[string]string `json:"default-for-task-types" pflag:"-,"`
}

type BackOffConfig struct {
	BaseSecond  int             `json:"base-second" pflag:",The number of seconds representing the base duration of the exponential backoff"`
	MaxDuration config.Duration `json:"max-duration" pflag:",The cap of the backoff duration"`
}

type PluginConfig struct {
	DefaultForTaskTypes []string
}

func cleanString(source string) string {
	cleaned := strings.Trim(source, " ")
	cleaned = strings.ToLower(cleaned)
	return cleaned
}

func (p TaskPluginConfig) GetEnabledPlugins() map[string]PluginConfig {
	enabledPlugins := make(map[string]PluginConfig)
	pluginDefaultForTaskType := map[string][]string{}
	// Reverse the map. Having the config use task type as a key guarantees only one default plugin can be specified per
	// task type but now we need to sort for which tasks a plugin needs to be the default.
	for taskName, pluginName := range p.DefaultForTaskTypes {
		pluginDefaultForTaskType[pluginName] = append(pluginDefaultForTaskType[pluginName], cleanString(taskName))
	}
	for _, pluginName := range p.EnabledPlugins {
		cleanedPluginName := cleanString(pluginName)
		enabledPlugins[cleanedPluginName] = PluginConfig{
			DefaultForTaskTypes: pluginDefaultForTaskType[pluginName],
		}
	}
	return enabledPlugins
}

func GetConfig() *Config {
	return section.GetConfig().(*Config)
}
