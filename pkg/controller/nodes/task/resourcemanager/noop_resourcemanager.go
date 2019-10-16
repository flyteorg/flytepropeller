package resourcemanager

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/resourcemanager_interface"
)

type NoopResourceManager struct {
}

func (r *NoopResourceManager) GetNegotiator(namespacePrefix resourcemanager_interface.ResourceNamespace) resourcemanager_interface.ResourceNegotiator {
	return Proxy{
		ResourceNegotiator: r,
		ResourceManager:    r,
		NamespacePrefix:    namespacePrefix,
	}
}

func (r *NoopResourceManager) GetTaskResourceManager(namespacePrefix resourcemanager_interface.ResourceNamespace) resourcemanager_interface.ResourceManager {
	return Proxy{
		ResourceNegotiator: r,
		ResourceManager:    r,
		NamespacePrefix:    namespacePrefix,
	}
}

func (*NoopResourceManager) RegisterResourceQuota(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace, quota int) error {
	return nil
}

func (*NoopResourceManager) AllocateResource(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace, allocationToken string) (
	resourcemanager_interface.AllocationStatus, error) {

	return resourcemanager_interface.AllocationStatusGranted, nil
}

func (*NoopResourceManager) ReleaseResource(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace, allocationToken string) error {
	return nil
}
