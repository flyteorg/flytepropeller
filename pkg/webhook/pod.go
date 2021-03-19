package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/flyteorg/flytepropeller/pkg/utils/secrets"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/flytek8s"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	corev1 "k8s.io/api/core/v1"
)

const webhookName = "flyte-pod-webhook.flyte.org"

type PodMutator struct {
	decoder  *admission.Decoder
	cfg      *Config
	Mutators []MutatorConfig
}

type MutatorConfig struct {
	Mutator  Mutator
	Required bool
}

type Mutator interface {
	ID() string
	Mutate(ctx context.Context, p *corev1.Pod) (newP *corev1.Pod, changed bool, err error)
}

func (pm *PodMutator) InjectClient(_ client.Client) error {
	return nil
}

// InjectDecoder injects the decoder into a mutatingHandler.
func (pm *PodMutator) InjectDecoder(d *admission.Decoder) error {
	pm.decoder = d
	return nil
}

func (pm *PodMutator) Handle(ctx context.Context, request admission.Request) admission.Response {
	// Get the object in the request
	obj := &corev1.Pod{}
	err := pm.decoder.Decode(request, obj)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	newObj, changed, err := pm.Mutate(ctx, obj)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if changed {
		marshalled, err := json.Marshal(newObj)
		if err != nil {
			return admission.Errored(http.StatusInternalServerError, err)
		}

		// Create the patch
		return admission.PatchResponseFromRaw(request.Object.Raw, marshalled)
	}

	return admission.Allowed("No changes")
}

func (pm PodMutator) Mutate(ctx context.Context, p *corev1.Pod) (newP *corev1.Pod, changed bool, err error) {
	newP = p
	for _, m := range pm.Mutators {
		tempP := newP
		tempChanged := false
		tempP, tempChanged, err = m.Mutator.Mutate(ctx, tempP)
		if err != nil {
			if m.Required {
				err = fmt.Errorf("failed to mutate using [%v]. Since it's a required mutator, failing early. Error: %v", m.Mutator.ID(), err)
				logger.Info(ctx, err)
				return p, false, err
			} else {
				logger.Infof(ctx, "Failed to mutate using [%v]. Since it's not a required mutator, skipping. Error: %v", m.Mutator.ID(), err)
				continue
			}
		}

		newP = tempP
		if tempChanged {
			changed = true
		}
	}

	return newP, changed, nil
}

func (pm *PodMutator) Register(ctx context.Context, mgr manager.Manager) error {
	wh := &admission.Webhook{
		Handler: pm,
	}

	mutatePath := getPodMutatePath()
	logger.Infof(ctx, "Registering path [%v]", mutatePath)
	mgr.GetWebhookServer().Register(mutatePath, wh)
	return nil
}

func (pm PodMutator) GetMutatePath() string {
	return getPodMutatePath()
}

func getPodMutatePath() string {
	pod := flytek8s.BuildIdentityPod()
	return generateMutatePath(pod.GroupVersionKind())
}

func generateMutatePath(gvk schema.GroupVersionKind) string {
	return "/mutate-" + strings.Replace(gvk.Group, ".", "-", -1) + "-" +
		gvk.Version + "-" + strings.ToLower(gvk.Kind)
}

func (pm PodMutator) CreateMutationWebhookConfiguration(namespace string) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	caBuff, err := ReadFile(filepath.Join(pm.cfg.CertDir, "ca.crt"))
	if err != nil {
		return nil, err
	}

	path := pm.GetMutatePath()
	fail := admissionregistrationv1.Ignore
	sideEffects := admissionregistrationv1.SideEffectClassNoneOnDryRun

	mutateConfig := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pm.cfg.ServiceName,
			Namespace: namespace,
		},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name: webhookName,
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					CABundle: caBuff.Bytes(), // CA bundle created earlier
					Service: &admissionregistrationv1.ServiceReference{
						Name:      pm.cfg.ServiceName,
						Namespace: namespace,
						Path:      &path,
					},
				},
				Rules: []admissionregistrationv1.RuleWithOperations{
					{
						Operations: []admissionregistrationv1.OperationType{
							admissionregistrationv1.Create,
						},
						Rule: admissionregistrationv1.Rule{
							APIGroups:   []string{"*"},
							APIVersions: []string{"v1"},
							Resources:   []string{"pods"},
						},
					},
				},
				FailurePolicy: &fail,
				SideEffects:   &sideEffects,
				AdmissionReviewVersions: []string{
					"v1",
					"v1beta1",
				},
				ObjectSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						secrets.PodLabel: secrets.PodLabelValue,
					},
				},
			}},
	}

	return mutateConfig, nil
}

func NewPodMutator(cfg *Config, scope promutils.Scope) *PodMutator {
	return &PodMutator{
		cfg: cfg,
		Mutators: []MutatorConfig{
			{
				Mutator: NewSecretsMutator(scope.NewSubScope("secrets")),
			},
		},
	}
}
