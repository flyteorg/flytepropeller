package webhook

import (
	"context"
	"fmt"
	"testing"

	"github.com/flyteorg/flytepropeller/pkg/webhook/config"

	"github.com/go-test/deep"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestVaultSecretManagerInjector_Inject(t *testing.T) {
	injector := NewVaultSecretManagerInjector(config.DefaultConfig.VaultSecretManagerConfig)

	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "container1",
				},
			},
		},
	}
	inputSecret := &core.Secret{
		Group: "foo",
		Key:   "bar",
	}

	actualP, injected, err := injector.Inject(context.Background(), inputSecret, p.DeepCopy())

	// Retrieve the uuid
	uuid := ""
	for k := range actualP.ObjectMeta.Annotations {
		if len(k) > 39 && k[:39] == "vault.hashicorp.com/agent-inject-secret" {
			uuid = k[40:]
		}
	}

	expected := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"vault.hashicorp.com/agent-inject":                              "true",
				"vault.hashicorp.com/secret-volume-path":                        "/etc/flyte/secrets",
				"vault.hashicorp.com/role":                                      "flyte",
				"vault.hashicorp.com/agent-pre-populate-only":                   "true",
				fmt.Sprintf("vault.hashicorp.com/agent-inject-secret-%s", uuid): "foo",
				fmt.Sprintf("vault.hashicorp.com/agent-inject-file-%s", uuid):   "foo/bar",
				fmt.Sprintf("vault.hashicorp.com/agent-inject-template-%s", uuid): `
		{{- with secret "foo" -}}
		{{ .Data.data.bar }}
		{{- end -}}`,
			},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{},
			Containers: []corev1.Container{
				{
					Name: "container1",
					Env: []corev1.EnvVar{
						{
							Name:  "FLYTE_SECRETS_DEFAULT_DIR",
							Value: "/etc/flyte/secrets",
						},
						{
							Name: "FLYTE_SECRETS_FILE_PREFIX",
						},
					},
				},
			},
		},
	}

	assert.NoError(t, err)
	assert.True(t, injected)
	if diff := deep.Equal(actualP, expected); diff != nil {
		assert.Fail(t, "actual != expected", "Diff: %v", diff)
	}
}
