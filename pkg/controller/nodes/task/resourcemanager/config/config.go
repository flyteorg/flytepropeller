package config

import (
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
	NextProtos                  []string `json:"nextProtos" pflag:",List of supported application level protocols, in order of preference."`
	ServerName                  string   `json:"serverName" pflag:",Used to verify the hostname on the returned certificates unless InsecureSkipVerify is given."`
	InsecureSkipVerify          bool     `json:"insecureSkipVerify" pflag:",Whether a client verifies the server's certificate chain and host name"`
	SessionTicketsDisabled      bool     `json:"sessionTicketsDisabled" pflag:",May be set to true to disable session ticket and PSK (resumption) support"`
	MinVersion                  uint16   `json:"minVersion" pflag:",Minimum TLS version that is acceptable"`
	MaxVersion                  uint16   `json:"maxVersion" pflag:",Maximum TLS version that is acceptable."`
	CurvePreferences            uint16   `json:"curvePreferences" pflag:",CurvePreferences contains the elliptic curves that will be used in an ECDHE handshake, in preference order. If empty, the default will be used. See https://pkg.go.dev/crypto/tls#CurveID."`
	DynamicRecordSizingDisabled bool     `json:"dynamicRecordSizingDisabled" pflag:",Disables adaptive sizing of TLS records.When true, the largest possible TLS record size is always used."`
	Renegotiation               int      `json:"renegotiation" pflag:",What type of renegotiations are supported. The default, none, is correct for most applications."`
}

// Retrieves the current config value or default.
func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}

func SetConfig(cfg *Config) error {
	return configSection.SetConfig(cfg)
}
