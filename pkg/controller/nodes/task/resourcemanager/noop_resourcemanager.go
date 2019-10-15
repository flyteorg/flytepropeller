package resourcemanager

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager_interface"
)

type NoopResourceManager struct {
}

func (r *NoopResourceManager) GetNegotiator() resourcemanager_interface.ResourceNegotiator {
	return r
}

func (r *NoopResourceManager) GetTaskResourceManager(ctx context.Context, tCtx core.TaskExecutionContext) resourcemanager_interface.ResourceManager {
	return r
}

func (NoopResourceManager) RegisterResourceQuota(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace, quota int) error {
	return nil
}

func (NoopResourceManager) AllocateResource(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace, allocationToken string) (
	resourcemanager_interface.AllocationStatus, error) {

	return resourcemanager_interface.AllocationStatusGranted, nil
}

func (NoopResourceManager) ReleaseResource(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace, allocationToken string) error {
	return nil
}
