package webhook

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
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
	v, err := g.envSecretManager.Get(ctx, secret.Key)
	if err != nil {
		return p, err
	}

	// Mount the secret to all containers in the given pod.
	for _, c := range append(p.Spec.InitContainers, p.Spec.Containers...) {
		prefixEnvExist := false
		for _, e := range c.Env {
			if e.Name == K8sEnvVarPrefix {
				prefixEnvExist = true
				break
			}
		}

		if !prefixEnvExist {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  K8sEnvVarPrefix,
				Value: "",
			})
		}

		c.Env = append(c.Env, corev1.EnvVar{
			Name:  secret.Group + "." + secret.Key,
			Value: v,
		})
	}

	return p, nil
}
