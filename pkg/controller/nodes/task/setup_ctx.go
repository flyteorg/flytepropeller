package task

import (
	"context"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager"

	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

type setupContext struct {
	handler.SetupContext
	kubeClient    pluginCore.KubeClient
	secretManager pluginCore.SecretManager
	resourceManager resourcemanager.ResourceManager
}

func (s setupContext) SecretManager() pluginCore.SecretManager {
	return s.secretManager
}

func (s setupContext) MetricsScope() promutils.Scope {
	return s.SetupContext.MetricsScope()
}

func (s setupContext) KubeClient() pluginCore.KubeClient {
	return s.kubeClient
}

func (s setupContext) EnqueueOwner() pluginCore.EnqueueOwner {
	return func(ownerId types.NamespacedName) error {
		s.SetupContext.EnqueueOwner()(ownerId.String())
		return nil
	}
}

func (t *Handler) newSetupContext(_ context.Context, sCtx handler.SetupContext) pluginCore.SetupContext {
	return &setupContext{
		SetupContext:  sCtx,
		kubeClient:    t.kubeClient,
		secretManager: t.secretManager,
		resourceManager: t.resourceManager,
	}
}
