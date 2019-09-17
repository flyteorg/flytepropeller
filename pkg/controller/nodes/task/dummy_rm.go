package task

import (
	"context"

	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

type dummyRM struct {

}

func (d dummyRM) AllocateResource(ctx context.Context, namespace string, allocationToken string) (pluginCore.AllocationStatus, error) {
	return pluginCore.AllocationStatusGranted, nil
}

func (d dummyRM) ReleaseResource(ctx context.Context, namespace string, allocationToken string) error {
	return nil
}

