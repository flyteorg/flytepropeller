package task

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

type dummyRM struct {
}

func (d dummyRM) AllocateResource(ctx context.Context, namespace core.ResourceNamespace, allocationToken string) (core.AllocationStatus, error) {
	return core.AllocationStatusGranted, nil
}

func (d dummyRM) ReleaseResource(ctx context.Context, namespace core.ResourceNamespace, allocationToken string) error {
	return nil
}
