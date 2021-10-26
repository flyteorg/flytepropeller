package config

import (
	"crypto/tls"
	"fmt"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
)

//go:generate pflags Config --default-var=defaultConfig

const configSectionKey = "resourcemanager"

type Type = string

const (
	TypeNoop  Type = "noop"
	TypeRedis Type = "redis"
)

var (
	defaultConfig = Config{
		Type: TypeNoop,
		// TODO: Noop Resource Manager doesn't use MaxQuota. Maybe we can remove it?
		ResourceMaxQuota: 1000,
	}

	configSection = config.MustRegisterSubSection(configSectionKey, &defaultConfig)
)

// Configs for Resource Manager
type Config struct {
	Type             Type        `json:"type" pflag:"noop,Which resource manager to use"`
	ResourceMaxQuota int         `json:"resourceMaxQuota" pflag:",Global limit for concurrent Qubole queries"`
	RedisConfig      RedisConfig `json:"redis" pflag:",Config for Redis resourcemanager."`
}

// Specific configs for Redis resource manager
// Ref: https://redis.io/topics/sentinel for information on how to fill in these fields.
type RedisConfig struct {
	HostPaths   []string `json:"hostPaths" pflag:",Redis hosts locations."`
	PrimaryName string   `json:"primaryName" pflag:",Redis primary name, fill in only if you are connecting to a redis sentinel cluster."`
	// deprecated: Please use HostPaths instead
	HostPath   string    `json:"hostPath" pflag:",Redis host location"`
	HostKey    string    `json:"hostKey" pflag:",Key for local Redis access"`
	MaxRetries int       `json:"maxRetries" pflag:",See Redis client options for more info"`
	TLSConfig  TLSConfig `json:"tlsConfig" pflag:",See the crytpo/tls config object for more info"`
}

// Specific configs for Redis TLS
// Ref: https://pkg.go.dev/crypto/tls#Config for informations on how to fill in these fields.
type TLSConfig struct {
	ServerName string `json:"serverName" pflag:",Used to verify the hostname."`
	MinVersion string `json:"minVersion" pflag:",Minimum TLS version that is acceptable"`
	MaxVersion string `json:"maxVersion" pflag:",Maximum TLS version that is acceptable."`
}

func GetTLSVersion(version string) uint16 {
	// Parses string version specifiers into correct tls Version specifier.
	// Returns zero if unsuccessful which tls.Config will interpret as respective
	// default for minimum or maximum version.
	switch version {
	case "1.0":
		return tls.VersionTLS10
	case "1.1":
		return tls.VersionTLS11
	case "1.2":
		return tls.VersionTLS12
	case "1.3":
		return tls.VersionTLS13
	}
	fmt.Printf("Received unsupported TLS Version %s, reverting to default TLS settings.", version)
	return 0
}

func ParseTLSConfig(cfg TLSConfig) *tls.Config {
	// Parses TLSConfig settings into a proper tls.Config object. Returns a pointer
	// to nil if no settings were manually defined (i.e. cfg is empty) to keep tls disabled.
	if cfg != (TLSConfig{}) {
		return &tls.Config{
			//Certificates []Certificate,
			//RootCAs *x509.CertPool,
			ServerName: cfg.ServerName,
			//ClientCAs *x509.CertPool,
			MinVersion: GetTLSVersion(cfg.MinVersion),
			MaxVersion: GetTLSVersion(cfg.MaxVersion),
		}
	}
	return nil
}

// Retrieves the current config value or default.
func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}
