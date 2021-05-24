package webhook

import "github.com/flyteorg/flytestdlib/config"

//go:generate enumer --type=SecretManagerType --trimprefix=SecretManagerType -json -yaml
//go:generate pflags Config --default-var=defaultConfig

var (
	defaultConfig = &Config{
		SecretName:        "flyte-pod-webhook",
		ServiceName:       "flyte-pod-webhook",
		MetricsPrefix:     "flyte:",
		CertDir:           "/etc/webhook/certs",
		ListenPort:        9443,
		SecretManagerType: SecretManagerTypeK8s,
	}

	configSection = config.MustRegisterSection("webhook", defaultConfig)
)

// SecretManagerType defines which secret manager to use.
type SecretManagerType int

const (
	SecretManagerTypeGlobal SecretManagerType = iota
	SecretManagerTypeK8s
	SecretManagerTypeAWS
)

type Config struct {
	MetricsPrefix     string            `json:"metrics-prefix" pflag:",An optional prefix for all published metrics."`
	CertDir           string            `json:"certDir" pflag:",Certificate directory to use to write generated certs. Defaults to /etc/webhook/certs/"`
	ListenPort        int               `json:"listenPort" pflag:",The port to use to listen to webhook calls. Defaults to 9443"`
	ServiceName       string            `json:"serviceName" pflag:",The name of the webhook service."`
	SecretName        string            `json:"secretName" pflag:",Secret name to write generated certs to."`
	SecretManagerType SecretManagerType `json:"secretManagerType" pflag:"-,Secret manager type to use if secrets are not found in global secrets."`
}

func GetConfig() *Config {
	return configSection.GetConfig().(*Config)
}
