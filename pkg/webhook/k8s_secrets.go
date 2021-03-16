package webhook

import (
	"context"
	"fmt"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytestdlib/logger"
	corev1 "k8s.io/api/core/v1"
)

const (
	K8sPathPrefixEnvVar    = "FLYTE_SECRETS_DEFAULT_DIR"
	K8sSecretPathPrefix    = "/etc/flyte/secrets/"
	K8sEnvVarPrefix        = "FLYTE_SECRETS_ENV_PREFIX"
	K8sDefaultEnvVarPrefix = "_FSEC_"
)

type K8sSecretInjector struct {
}

func (i K8sSecretInjector) ID() string {
	return "K8s"
}

func UpdateVolumeMounts(containers []corev1.Container, secretName string) []corev1.Container {
	res := make([]corev1.Container, 0, len(containers))
	for _, c := range containers {
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			Name:      secretName,
			ReadOnly:  true,
			MountPath: K8sSecretPathPrefix + secretName,
		})

		res = append(res, c)
	}

	return res
}

func UpdateEnvVars(containers []corev1.Container, envVar corev1.EnvVar) []corev1.Container {
	res := make([]corev1.Container, 0, len(containers))
	for _, c := range containers {
		c.Env = append(c.Env, envVar)
		res = append(res, c)
	}

	return res
}

func (i K8sSecretInjector) Inject(ctx context.Context, secret *core.Secret, p *corev1.Pod) (*corev1.Pod, error) {
	switch secret.MountRequirement {
	case core.Secret_ANY:
		fallthrough
	case core.Secret_FILE:
		volumeName := secret.Key
		if len(secret.Group) > 0 {
			volumeName = secret.Group + "_" + secret.Key
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
		} else {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: secret.Key,
					},
				},
			})
		}

		// Mount the secret to all containers in the given pod.
		p.Spec.InitContainers = UpdateVolumeMounts(p.Spec.InitContainers, volumeName)
		p.Spec.Containers = UpdateVolumeMounts(p.Spec.Containers, volumeName)

		prefixEnvVar := corev1.EnvVar{
			Name:  K8sPathPrefixEnvVar,
			Value: K8sSecretPathPrefix,
		}

		p.Spec.InitContainers = UpdateEnvVars(p.Spec.InitContainers, prefixEnvVar)
		p.Spec.Containers = UpdateEnvVars(p.Spec.Containers, prefixEnvVar)
	case core.Secret_ENV_VAR:
		if len(secret.Group) == 0 {
			return nil, fmt.Errorf("mounting a secret to env var requires selecting the secret and a single key within. Key [%v]", secret.Key)
		}

		envVar := corev1.EnvVar{
			Name: secret.Group + "_" + secret.Key,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: secret.Group,
					},
					Key: secret.Key,
				},
			},
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
