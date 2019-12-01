package containercompletion

import (
	"context"
	"fmt"

	"github.com/lyft/flytestdlib/logger"
	v13 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes/typed/core/v1"
)

type kubeAPIWatcher struct {
	coreClient v1.CoreV1Interface
}

func (k kubeAPIWatcher) WaitForContainerToComplete(ctx context.Context, info ContainerInformation) error {
	s := fields.OneTermEqualSelector("metadata.name", info.PodName)
	watcher, err := k.coreClient.Pods(info.Namespace).Watch(v12.ListOptions{
		Watch:         true,
		FieldSelector: s.String(),
	})
	if err != nil {
		return err
	}
	defer watcher.Stop()
	for {
		select {
		case v := <-watcher.ResultChan():
			p := v.Object.(*v13.Pod)
			for _, c := range p.Status.ContainerStatuses {
				if c.Name == info.Name {
					// TODO we may want to check failure reason and return that?
					if c.State.Terminated != nil {
						if c.State.Terminated.ExitCode != 0 {
							return fmt.Errorf("Container exited with Exit code [%d]. Reason [%s]%s. ", c.State.Terminated.ExitCode, c.State.Terminated.Reason, c.State.Terminated.Message)
						}
						return nil
					}
				}
			}
		case <-ctx.Done():
			logger.Infof(ctx, "Pod [%s/%s] watcher canceled", info.Namespace, info.PodName)
			return nil
		}
	}
}

func NewKubeAPIWatcher(_ context.Context, coreClient v1.CoreV1Interface) (Watcher, error) {
	return kubeAPIWatcher{coreClient: coreClient}, nil
}
