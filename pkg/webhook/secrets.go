package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"

	"github.com/flyteorg/flytestdlib/logger"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	secretUtils "github.com/flyteorg/flytepropeller/pkg/utils/secrets"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	corev1 "k8s.io/api/core/v1"
)

type SecretsWebhook struct {
	decoder   *admission.Decoder
	injectors []SecretsInjector
}

type SecretsInjector interface {
	ID() string
	Inject(ctx context.Context, secrets *core.Secret, p *corev1.Pod) (*corev1.Pod, error)
}

func (s *SecretsWebhook) InjectClient(c client.Client) error {
	return nil
}

// InjectDecoder injects the decoder into a mutatingHandler.
func (s *SecretsWebhook) InjectDecoder(d *admission.Decoder) error {
	s.decoder = d
	return nil
}

func (s *SecretsWebhook) Handle(ctx context.Context, request admission.Request) admission.Response {
	// Get the object in the request
	obj := &corev1.Pod{}
	err := s.decoder.Decode(request, obj)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	secrets, err := secretUtils.UnmarshalStringMapToSecrets(obj.GetAnnotations())
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	for _, secret := range secrets {
		for _, injector := range s.injectors {
			obj, err = injector.Inject(ctx, secret, obj)
			if err != nil {
				logger.Infof(ctx, "Failed to inject a secret using injector [%v]. Error: %v", injector.ID(), err)
			} else {
				break
			}
		}

		if err != nil {
			return admission.Errored(http.StatusBadRequest, err)
		}
	}

	marshalled, err := json.Marshal(obj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Create the patch
	return admission.PatchResponseFromRaw(request.Object.Raw, marshalled)
}

func (s *SecretsWebhook) Register(ctx context.Context, mgr manager.Manager) error {
	wh := &admission.Webhook{
		Handler: s,
	}

	mutatePath := GetPodMutatePath()
	logger.Infof(ctx, "Registering path [%v]", mutatePath)
	mgr.GetWebhookServer().Register(mutatePath, wh)
	return nil
}

func (s SecretsWebhook) GetMutatePath() string {
	return GetPodMutatePath()
}

func GetPodMutatePath() string {
	pod := flytek8s.BuildIdentityPod()
	return generateMutatePath(pod.GroupVersionKind())
}

func generateMutatePath(gvk schema.GroupVersionKind) string {
	return "/mutate-" + strings.Replace(gvk.Group, ".", "-", -1) + "-" +
		gvk.Version + "-" + strings.ToLower(gvk.Kind)
}

func NewSecretsWebhook(scope promutils.Scope) *SecretsWebhook {
	return &SecretsWebhook{
		injectors: []SecretsInjector{
			GlobalSecrets{},
			K8sSecretInjector{},
		},
	}
}
