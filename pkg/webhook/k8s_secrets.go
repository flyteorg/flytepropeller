package webhook

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	corev1 "k8s.io/api/core/v1"
)

const K8sSecretPathPrefix = "/etc/flyte/secrets/"

type K8sSecretInjector struct {
}

func (i K8sSecretInjector) Inject(ctx context.Context, secrets []*core.Secret, p *corev1.Pod) (*corev1.Pod, error) {
	for _, s := range secrets {
		p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
			Name: s.Name,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: s.Name,
				},
			},
		})

		// Mount the secret to all containers in the given pod.
		for _, c := range append(p.Spec.InitContainers, p.Spec.Containers...) {
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
				Name:      s.Name,
				ReadOnly:  true,
				MountPath: K8sSecretPathPrefix + s.Name,
			})
		}
	}

	return p, nil
}
