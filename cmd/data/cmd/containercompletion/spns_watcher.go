package containercompletion

import (
	"context"
	"fmt"
)

// The best option for this is to use https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/
// This is only available as Beta as of 1.16, so we will launch with this feature only as beta
// But this is the most efficient way to monitor the pod
type sharedProcessNSWatcher struct {
}

func (k sharedProcessNSWatcher) WaitForContainerToComplete(ctx context.Context, information ContainerInformation) error {
	return fmt.Errorf("process namesapce sharing is not yet implemented")
}

func NewSharedProcessNSWatcher(_ context.Context) (Watcher, error) {
	return sharedProcessNSWatcher{}, nil
}
