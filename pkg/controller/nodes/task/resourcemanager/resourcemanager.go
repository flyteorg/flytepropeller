package resourcemanager

import (
	"context"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/resourcemanager_interface"
	"github.com/lyft/flytestdlib/promutils"
)

//go:generate mockery -name ResourceManager -case=underscore

type Type string

const (
	TypeNoop  Type = "noop"
	TypeRedis Type = "redis"
)



// This struct is designed to serve as the identifier of an user of resource manager
type Resource struct {
	quota   int
	metrics Metrics
}

type Metrics interface {
	GetScope() promutils.Scope
}

type Factory interface {
	GetNegotiator(namespacePrefix resourcemanager_interface.ResourceNamespace) resourcemanager_interface.ResourceNegotiator
	GetTaskResourceManager(namespacePrefix resourcemanager_interface.ResourceNamespace) resourcemanager_interface.ResourceManager
}

type Proxy struct {
	resourcemanager_interface.ResourceNegotiator
	resourcemanager_interface.ResourceManager
	NamespacePrefix resourcemanager_interface.ResourceNamespace
}

func (p Proxy) getPrefixedNamespace(namespace resourcemanager_interface.ResourceNamespace) resourcemanager_interface.ResourceNamespace {
	return p.NamespacePrefix.CreateSubNamespace(namespace)
}

func (p Proxy) RegisterResourceQuota(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace,
	quota int) error {
	return p.ResourceNegotiator.RegisterResourceQuota(ctx, p.getPrefixedNamespace(namespace), quota)
}

func (p Proxy) AllocateResource(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace,
	allocationToken string) (resourcemanager_interface.AllocationStatus, error) {
	status, err := p.ResourceManager.AllocateResource(ctx, p.getPrefixedNamespace(namespace), allocationToken)
	return status, err
}

func (p Proxy) ReleaseResource(ctx context.Context, namespace resourcemanager_interface.ResourceNamespace,
	allocationToken string) error {
	err := p.ResourceManager.ReleaseResource(ctx, p.getPrefixedNamespace(namespace), allocationToken)
	return err
}

