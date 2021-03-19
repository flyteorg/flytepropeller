package webhook

import (
	"context"
	"fmt"
	"strings"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytestdlib/logger"
	corev1 "k8s.io/api/core/v1"
)

const (
	K8sPathDefaultDirEnvVar = "FLYTE_SECRETS_DEFAULT_DIR"
	K8sPathFilePrefixEnvVar = "FLYTE_SECRETS_FILE_PREFIX"
	K8sSecretPathPrefix     = "/etc/flyte/secrets/"
	K8sEnvVarPrefix         = "FLYTE_SECRETS_ENV_PREFIX"
	K8sDefaultEnvVarPrefix  = "_FSEC_"
	EnvVarGroupKeySeparator = "_"
)

type K8sSecretInjector struct {
}

func (i K8sSecretInjector) ID() string {
	return "K8s"
}

func (i K8sSecretInjector) Inject(ctx context.Context, secret *core.Secret, p *corev1.Pod) (newP *corev1.Pod, injected bool, err error) {
	switch secret.MountRequirement {
	case core.Secret_ANY:
		fallthrough
	case core.Secret_FILE:
		volumeName := secret.Key
		if len(secret.Group) > 0 && len(secret.Key) > 0 {
			volumeName = secret.Group + EnvVarGroupKeySeparator + secret.Key
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: secret.Group,
						Items: []corev1.KeyToPath{
							{
								Key:  secret.Key,
								Path: secret.Key,
							},
						},
					},
				},
			})
		} else if len(secret.Key) > 0 {
			volumeName = secret.Key
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: secret.Key,
					},
				},
			})
		} else if len(secret.Group) > 0 {
			volumeName = secret.Group
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: secret.Group,
					},
				},
			})
		}

		// Mount the secret to all containers in the given pod.
		p.Spec.InitContainers = UpdateVolumeMounts(p.Spec.InitContainers, volumeName)
		p.Spec.Containers = UpdateVolumeMounts(p.Spec.Containers, volumeName)

		defaultDirEnvVar := corev1.EnvVar{
			Name:  K8sPathDefaultDirEnvVar,
			Value: K8sSecretPathPrefix,
		}

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, defaultDirEnvVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, defaultDirEnvVar)

		prefixEnvVar := corev1.EnvVar{
			Name:  K8sPathFilePrefixEnvVar,
			Value: "",
		}

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, prefixEnvVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, prefixEnvVar)
	case core.Secret_ENV_VAR:
		if len(secret.Group) == 0 {
			return nil, false, fmt.Errorf("mounting a secret to env var requires selecting the secret and a single key within. Key [%v]", secret.Key)
		}

		envVar := corev1.EnvVar{
			Name: strings.ToUpper(K8sDefaultEnvVarPrefix + secret.Group + EnvVarGroupKeySeparator + secret.Key),
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secret.Group,
					},
					Key: secret.Key,
				},
			},
		}

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, envVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, envVar)

		prefixEnvVar := corev1.EnvVar{
			Name:  K8sEnvVarPrefix,
			Value: K8sDefaultEnvVarPrefix,
		}

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, prefixEnvVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, prefixEnvVar)
	default:
		err := fmt.Errorf("unrecognized mount requirement [%v] for secret [%v]", secret.MountRequirement.String(), secret.Key)
		logger.Error(ctx, err)
		return p, false, err
	}

	return p, true, nil
}

func NewK8sSecretsInjector() K8sSecretInjector {
	return K8sSecretInjector{}
}
