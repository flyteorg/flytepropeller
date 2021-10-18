package webhook

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/utils"
	"github.com/flyteorg/flytepropeller/pkg/webhook/config"
	"github.com/flyteorg/flytestdlib/logger"
	corev1 "k8s.io/api/core/v1"
)

var (
	VaultSecretPathPrefix = []string{string(os.PathSeparator), "etc", "flyte", "secrets"}
)

// VaultSecretInjector allows injecting of secrets into pods by specifying either EnvVarSource or SecretVolumeSource in
// the Pod Spec. It'll, by default, mount secrets as files into pods.
// The current version does not allow mounting an entire secret object (with all keys inside it). It only supports mounting
// a single key from the referenced secret object.
// The secret.Group will be used to reference the k8s secret object, the Secret.Key will be used to reference a key inside
// and the secret.Version will be ignored.
// Environment variables will be named _FSEC_<SecretGroup>_<SecretKey>. Files will be mounted on
// /etc/flyte/secrets/<SecretGroup>/<SecretKey>
type VaultSecretInjector struct {
	cfg config.VaultSecretManagerConfig
}

func (i VaultSecretInjector) Type() config.SecretManagerType {
	return config.SecretManagerTypeVault
}

func (i VaultSecretInjector) Inject(ctx context.Context, secret *core.Secret, p *corev1.Pod) (newP *corev1.Pod, injected bool, err error) {
	if len(secret.Group) == 0 || len(secret.Key) == 0 {
		return nil, false, fmt.Errorf("Vault Secrets Webhook requires both key and group to be set. "+
			"Secret: [%v]", secret)
	}

	switch secret.MountRequirement {
	case core.Secret_ANY:
		// Set environment variable to let the container know where to find the mounted files.
		defaultDirEnvVar := corev1.EnvVar{
			Name:  SecretPathDefaultDirEnvVar,
			Value: filepath.Join(VaultSecretPathPrefix...),
		}

		p.Spec.InitContainers = AppendEnvVars(p.Spec.InitContainers, defaultDirEnvVar)
		p.Spec.Containers = AppendEnvVars(p.Spec.Containers, defaultDirEnvVar)

		// Sets an empty prefix to let the containers know the file names will match the secret keys as-is.
		prefixEnvVar := corev1.EnvVar{
			Name:  SecretPathFilePrefixEnvVar,
			Value: "",
		}

		p.Spec.InitContainers = AppendEnvVars(p.Spec.InitContainers, prefixEnvVar)
		p.Spec.Containers = AppendEnvVars(p.Spec.Containers, prefixEnvVar)

		generalVaultAnnotations := map[string]string{
			"vault.hashicorp.com/agent-inject":            "true",
			"vault.hashicorp.com/secret-volume-path":      filepath.Join(VaultSecretPathPrefix...),
			"vault.hashicorp.com/role":                    i.cfg.Role,
			"vault.hashicorp.com/agent-pre-populate-only": "true",
		}

		secretVaultAnnotations := CreateAnnotationsForSecret(secret)

		p.ObjectMeta.Annotations = utils.UnionMaps(p.ObjectMeta.Annotations, generalVaultAnnotations)
		p.ObjectMeta.Annotations = utils.UnionMaps(p.ObjectMeta.Annotations, secretVaultAnnotations)

		fmt.Println(p.ObjectMeta.Annotations)

	default:
		err := fmt.Errorf("unrecognized mount requirement [%v] for secret [%v]", secret.MountRequirement.String(), secret.Key)
		logger.Error(ctx, err)
		return p, false, err
	}

	return p, true, nil
}

func NewVaultSecretsInjector(cfg config.VaultSecretManagerConfig) VaultSecretInjector {
	return VaultSecretInjector{
		cfg: cfg,
	}
}
