package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytestdlib/logger"
	corev1 "k8s.io/api/core/v1"

	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/secretmanager"
)

type GlobalSecrets struct {
	envSecretManager secretmanager.FileEnvSecretManager
}

func (g GlobalSecrets) ID() string {
	return "global"
}

func (g GlobalSecrets) Inject(ctx context.Context, secret *core.Secret, p *corev1.Pod) (*corev1.Pod, error) {
	v, err := g.envSecretManager.GetForSecret(ctx, secret)
	if err != nil {
		return p, err
	}

	switch secret.MountRequirement {
	case core.Secret_FILE:
		return nil, fmt.Errorf("cannot use FILE requirement with global secret [%v/%v]", secret.Group, secret.Key)
	case core.Secret_ANY:
		fallthrough
	case core.Secret_ENV_VAR:
		if len(secret.Group) == 0 {
			return nil, fmt.Errorf("mounting a secret to env var requires selecting the secret and a single key within. Key [%v]", secret.Key)
		}

		envVar := corev1.EnvVar{
			Name:  strings.ToUpper(K8sDefaultEnvVarPrefix + secret.Group + EnvVarGroupKeySeparator + secret.Key),
			Value: v,
		}

		prefixEnvVar := corev1.EnvVar{
			Name:  K8sEnvVarPrefix,
			Value: K8sDefaultEnvVarPrefix,
		}

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, prefixEnvVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, prefixEnvVar)

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, envVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, envVar)
	default:
		err := fmt.Errorf("unrecognized mount requirement [%v] for secret [%v]", secret.MountRequirement.String(), secret.Key)
		logger.Error(ctx, err)
		return p, err
	}

	return p, nil
}
