package webhook

import (
	"context"
	"encoding/json"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"net/http"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type SecretsWebhook struct {
	decoder *admission.Decoder
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

	// Default the object
	marshalled, err := json.Marshal(obj)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Create the patch
	return admission.PatchResponseFromRaw(request.Object.Raw, marshalled)
}

func (s SecretsWebhook) InjectCache(cache cache.Cache) error {
	panic("implement me")
}

func (s SecretsWebhook) InjectClient(client client.Client) error {
	panic("implement me")
}

func (s SecretsWebhook) Register(_ context.Context, mgr manager.Manager) error {
	wh := &admission.Webhook{
		Handler:s,
	}

	pod := &corev1.Pod{}
	mgr.GetWebhookServer().Register(generateMutatePath(pod.GroupVersionKind()), wh)
	return nil
}

func generateMutatePath(gvk schema.GroupVersionKind) string {
	return "/mutate-" + strings.Replace(gvk.Group, ".", "-", -1) + "-" +
		gvk.Version + "-" + strings.ToLower(gvk.Kind)
}

func NewSecretsWebhook() SecretsWebhook {
	return SecretsWebhook{}
}
