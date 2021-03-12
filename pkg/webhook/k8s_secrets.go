package webhook

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	corev1 "k8s.io/api/core/v1"
)

const (
	K8sSecretPathPrefix = "/etc/flyte/secrets/"
	K8sEnvVarPrefix     = "FLYTE_SECRETS_ENV_PREFIX"
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

func (i K8sSecretInjector) Inject(ctx context.Context, secret *core.Secret, p *corev1.Pod) (*corev1.Pod, error) {
	p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
		Name: secret.Name,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: secret.Name,
			},
		},
	})

	// Mount the secret to all containers in the given pod.
	p.Spec.InitContainers = UpdateVolumeMounts(p.Spec.InitContainers, secret.Name)
	p.Spec.Containers = UpdateVolumeMounts(p.Spec.Containers, secret.Name)

	return p, nil
}
