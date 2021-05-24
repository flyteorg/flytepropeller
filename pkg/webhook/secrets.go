package webhook

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/secretmanager"

	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	secretUtils "github.com/flyteorg/flytepropeller/pkg/utils/secrets"

	corev1 "k8s.io/api/core/v1"
)

const (
	SecretPathDefaultDirEnvVar = "FLYTE_SECRETS_DEFAULT_DIR"
	SecretPathFilePrefixEnvVar = "FLYTE_SECRETS_FILE_PREFIX"
	SecretEnvVarPrefix         = "FLYTE_SECRETS_ENV_PREFIX"
)

type SecretsMutator struct {
	cfg       *Config
	injectors []SecretsInjector
}

type SecretsInjector interface {
	Type() SecretManagerType
	Inject(ctx context.Context, secrets *core.Secret, p *corev1.Pod) (newP *corev1.Pod, injected bool, err error)
}

func (s SecretsMutator) ID() string {
	return "secrets"
}

func (s *SecretsMutator) Mutate(ctx context.Context, p *corev1.Pod) (newP *corev1.Pod, injected bool, err error) {
	secrets, err := secretUtils.UnmarshalStringMapToSecrets(p.GetAnnotations())
	if err != nil {
		return p, false, err
	}

	for _, secret := range secrets {
		for _, injector := range s.injectors {
			if injector.Type() != SecretManagerTypeGlobal && injector.Type() == s.cfg.SecretManagerType {
				logger.Infof(ctx, "Skipping SecretManager [%v] since it's not enabled.", injector.Type())
				continue
			}

			p, injected, err = injector.Inject(ctx, secret, p)
			if err != nil {
				logger.Infof(ctx, "Failed to inject a secret using injector [%v]. Error: %v", injector.Type(), err)
			} else if injected {
				break
			}
		}

		if err != nil {
			return p, false, err
		}
	}

	return p, injected, nil
}

func NewSecretsMutator(cfg *Config, _ promutils.Scope) *SecretsMutator {
	return &SecretsMutator{
		cfg: cfg,
		injectors: []SecretsInjector{
			NewGlobalSecrets(secretmanager.NewFileEnvSecretManager(secretmanager.GetConfig())),
			NewK8sSecretsInjector(),
			NewAWSSecretManagerInjector(),
		},
	}
}
