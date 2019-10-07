package resourcemanager

import (
	"context"
)

type NoopResourceManager struct {
}

func (NoopResourceManager) RegisterResourceRequest(ctx context.Context, namespace string, allocationToken string, quota int) error {
	return nil
}

func (NoopResourceManager) AllocateResource(ctx context.Context, namespace string, allocationToken string) (
	AllocationStatus, error) {

	return AllocationStatusGranted, nil
}

func (NoopResourceManager) ReleaseResource(ctx context.Context, namespace string, allocationToken string) error {
	return nil
}
