package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	secretUtils "github.com/flyteorg/flytepropeller/pkg/utils/secrets"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	corev1 "k8s.io/api/core/v1"
)

type SecretsWebhook struct {
	decoder  *admission.Decoder
	injector SecretsInjector
}

type SecretsInjector interface {
	Inject(ctx context.Context, secrets []*core.Secret, p *corev1.Pod) (*corev1.Pod, error)
}

// InjectDecoder injects the decoder into a mutatingHandler.
func (s *SecretsWebhook) InjectDecoder(d *admission.Decoder) error {
	s.decoder = d
	return nil
}

func (s SecretsWebhook) Handle(ctx context.Context, request admission.Request) admission.Response {
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

	newObj, err := s.injector.Inject(ctx, secrets, obj)
	if err != nil {
		admission.Errored(http.StatusBadRequest, err)
	}

	// Default the object
	marshalled, err := json.Marshal(newObj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Create the patch
	return admission.PatchResponseFromRaw(request.Object.Raw, marshalled)
}

func (s SecretsWebhook) Register(_ context.Context, mgr manager.Manager) error {
	wh := &admission.Webhook{
		Handler: s,
	}

	pod := &corev1.Pod{}
	mgr.GetWebhookServer().Register(generateMutatePath(pod.GroupVersionKind()), wh)
	return nil
}

func generateMutatePath(gvk schema.GroupVersionKind) string {
	return "/mutate-" + strings.Replace(gvk.Group, ".", "-", -1) + "-" +
		gvk.Version + "-" + strings.ToLower(gvk.Kind)
}

func NewSecretsWebhook(scope promutils.Scope) SecretsWebhook {
	return SecretsWebhook{}
}
