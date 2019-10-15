package task

import (
	"context"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager_interface"

	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

type setupContext struct {
	handler.SetupContext
	kubeClient             pluginCore.KubeClient
	secretManager          pluginCore.SecretManager
	resourceNegotiator 	   resourcemanager_interface.ResourceNegotiator
}

func  (s setupContext) GetNegotiator() resourcemanager_interface.ResourceNegotiator {
	return s.resourceNegotiator
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

func (t *Handler) newSetupContext(ctx context.Context, sCtx handler.SetupContext) (*setupContext, error) {
	return &setupContext{
		SetupContext:  sCtx,
		kubeClient:    t.kubeClient,
		secretManager: t.secretManager,
	}, nil
}
