package containercompletion

import (
	"context"
	"fmt"

	"github.com/fsnotify/fsnotify"
	"github.com/lyft/flytestdlib/logger"
)

type successFileWatcher struct {
	successFilePath string
}

func (k successFileWatcher) WaitForContainerToComplete(ctx context.Context, information ContainerInformation) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() {
		err := w.Close()
		if err != nil {
			logger.Errorf(context.TODO(), "failed to close file watcher")
		}
	}()
	done := make(chan error)
	go func() {
		for {
			select {
			case event, ok := <-w.Events:
				if !ok {
					done <- fmt.Errorf("failed to watch")
					return
				}
				if event.Op == fsnotify.Create || event.Op == fsnotify.Write {
					done <- nil
					return
				}
			case err, ok := <-w.Errors:
				if !ok {
					done <- fmt.Errorf("failed to watch")
					return
				}
				done <- err
				return
			}
		}
	}()
	if err := w.Add(k.successFilePath); err != nil {
		return err
	}
	return <-done
}

func NewSuccessFileWatcher(_ context.Context, successFilePath string) (Watcher, error) {
	return successFileWatcher{successFilePath: successFilePath}, nil
}
