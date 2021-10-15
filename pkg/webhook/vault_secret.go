package webhook

import (
	"context"
	"fmt"
	"os"
	"strings"
	//"path/filepath"

	"github.com/flyteorg/flytepropeller/pkg/webhook/config"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytestdlib/logger"
	corev1 "k8s.io/api/core/v1"
)

const (
	VaultDefaultEnvVarPrefix  = "_FSEC_"
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
}

func (i VaultSecretInjector) Type() config.SecretManagerType {
	return config.SecretManagerTypeVault
}

func (i VaultSecretInjector) Inject(ctx context.Context, secret *core.Secret, p *corev1.Pod) (newP *corev1.Pod, injected bool, err error) {
	if len(secret.Group) == 0 || len(secret.Key) == 0 {
		return nil, false, fmt.Errorf("Vault Secrets Webhook require both key and group to be set. "+
			"Secret: [%v]", secret)
	}

	switch secret.MountRequirement {
	case core.Secret_ANY:
		fmt.Println("DEBUG: We're doing this, YEAH!")
		
		//"vault.hashicorp.com/agent-inject": true
		// This is where we need to pick up stuff from vault			
		envVar := corev1.EnvVar{
			Name: strings.ToUpper(VaultDefaultEnvVarPrefix + secret.Group + EnvVarGroupKeySeparator + secret.Key),
			Value: "Mumintroll",
		}

		p.Spec.InitContainers = AppendEnvVars(p.Spec.InitContainers, envVar)
		p.Spec.Containers = AppendEnvVars(p.Spec.Containers, envVar)

		fmt.Println("Container spec")
		fmt.Println(p.Spec.Containers)
		fmt.Println("Init container spec")
		fmt.Println(p.Spec.InitContainers)

	default:
		err := fmt.Errorf("unrecognized mount requirement [%v] for secret [%v]", secret.MountRequirement.String(), secret.Key)
		logger.Error(ctx, err)
		return p, false, err
	}

	return p, true, nil
}

func NewVaultSecretsInjector() VaultSecretInjector {
	return VaultSecretInjector{}
}
