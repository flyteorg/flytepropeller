package containercompletion

import (
	"context"
)

type ContainerInformation struct {
	Namespace string
	PodName   string
	Name      string
}

type Watcher interface {
	WaitForContainerToComplete(ctx context.Context, information ContainerInformation) error
}

type WatcherType = string

const (
	// Uses KubeAPI to determine if the container is completed
	WatcherTypeKubeAPI WatcherType = "kube-api"
	// Uses a success file to determine if the container has completed.
	// CAUTION: Does not work if the container exits because of OOM, etc
	WatcherTypeSuccessFile WatcherType = "success-file"
	// Uses Kube 1.16 Beta feature - https://kubernetes.io/docs/tasks/configure-pod-container/share-process-namespace/
	// To look for pid in the shared namespace.
	WatcherTypeSharedProcessNS WatcherType = "shared-process-ns"
)

var AllWatcherTypes = []WatcherType{
	WatcherTypeKubeAPI,
	WatcherTypeSharedProcessNS,
	WatcherTypeSuccessFile,
}
