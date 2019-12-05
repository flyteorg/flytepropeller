package containercompletion

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/fsnotify/fsnotify"
	"github.com/lyft/flytestdlib/logger"
	"github.com/pkg/errors"
)

type successFileWatcher struct {
	watchDir    string
	successFile string
}

func FileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		if os.IsPermission(err) {
			return false, errors.Wrapf(err, "unable to read file [%s], Flyte does not have permissions", filePath)
		}
		return false, errors.Wrapf(err, "failed to read file")
	}
	return true, nil
}

func (k successFileWatcher) WaitForContainerToComplete(ctx context.Context, information ContainerInformation) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return errors.Wrapf(err, "failed to create watcher")
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
				logger.Infof(ctx, "event for :%s", event)
				if event.Name == k.successFile && (event.Op == fsnotify.Create || event.Op == fsnotify.Write) {
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
	if err := w.Add(k.watchDir); err != nil {
		return errors.Wrapf(err, "failed to add dir: %s to watcher,", k.watchDir)
	}
	if ok, err := FileExists(k.successFile); err != nil {
		logger.Errorf(ctx, "Failed to check existence of file, err: %s", err)
		return err
	} else if ok {
		logger.Infof(ctx, "File Already exists")
		return nil
	}
	return <-done
}

func NewSuccessFileWatcher(_ context.Context, watchDir, successFileName string) (Watcher, error) {
	return successFileWatcher{successFile: path.Join(watchDir, successFileName), watchDir: watchDir}, nil
}
