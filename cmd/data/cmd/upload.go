package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/lyft/flytepropeller/cmd/data/cmd/containerwatcher"
	"github.com/lyft/flytepropeller/data"
)

const (
	StartFile   = "_START"
	SuccessFile = "_SUCCESS"
	ErrorFile   = "_ERROR"
)

type UploadOptions struct {
	*RootOptions
	remoteOutputsPrefix  string
	remoteOutputsSandbox string
	localDirectoryPath   string
	// Non primitive types will be dumped in this output format
	outputFormat          data.Format
	timeout               time.Duration
	containerStartTimeout time.Duration
	outputInterface       []byte
	startWatcherType      containerwatcher.WatcherType
	exitWatcherType       containerwatcher.WatcherType
	containerInfo         containerwatcher.ContainerInformation
}

func (u *UploadOptions) createWatcher(ctx context.Context, w containerwatcher.WatcherType) (containerwatcher.Watcher, error) {
	switch w {
	case containerwatcher.WatcherTypeKubeAPI:
		// TODO, in this case container info should have namespace and podname and we can get it using downwardapi
		// TODO https://kubernetes.io/docs/tasks/inject-data-application/environment-variable-expose-pod-information/
		return containerwatcher.NewKubeAPIWatcher(ctx, u.RootOptions.kubeClient.CoreV1(), u.containerInfo)
	case containerwatcher.WatcherTypeFile:
		return containerwatcher.NewSuccessFileWatcher(ctx, u.localDirectoryPath, StartFile, SuccessFile, ErrorFile)
	case containerwatcher.WatcherTypeSharedProcessNS:
		return containerwatcher.NewSharedProcessNSWatcher(ctx, time.Second*2, 2)
	}
	return nil, fmt.Errorf("unsupported watcher type")
}

func (u *UploadOptions) uploader(ctx context.Context) error {
	if u.outputInterface == nil {
		logger.Infof(ctx, "No output interface provided. Assuming Void outputs.")
		return nil
	}

	outputInterface := &core.VariableMap{}
	if err := proto.Unmarshal(u.outputInterface, outputInterface); err != nil {
		logger.Errorf(ctx, "Bad output interface passed, failed to unmarshal err :%s", err)
		return errors.Wrap(err, "Bad output interface passed, failed to unmarshal")
	}

	if outputInterface.Variables == nil || len(outputInterface.Variables) == 0 {
		logger.Infof(ctx, "Empty output interface received. Assuming void outputs.")
		return nil
	}

	logger.Infof(ctx, "Creating start watcher type: %s", u.startWatcherType)
	w, err := u.createWatcher(ctx, u.startWatcherType)
	if err != nil {
		return err
	}

	logger.Infof(ctx, "Waiting for Container to start with timeout %s.", u.containerStartTimeout)
	childCtx, _ := context.WithTimeout(ctx, u.containerStartTimeout)
	err = w.WaitToStart(childCtx)
	if err != nil && err != containerwatcher.TimeoutError {
		return err
	}

	if err != nil {
		logger.Warnf(ctx, "Container start detection aborted, :%s", err.Error())
	}

	if u.startWatcherType != u.exitWatcherType {
		logger.Infof(ctx, "Creating watcher type: %s", u.exitWatcherType)
		w, err = u.createWatcher(ctx, u.exitWatcherType)
		if err != nil {
			return err
		}
	}

	logger.Infof(ctx, "Waiting for Container to exit.")
	if err := w.WaitToExit(ctx); err != nil {
		logger.Errorf(ctx, "Failed waiting for container to exit. Err: %s", err)
		return err
	}

	logger.Infof(ctx, "Container Exited! uploading data.")

	dl := data.NewUploader(ctx, u.Store, u.outputFormat, ErrorFile)
	childCtx, _ = context.WithTimeout(ctx, u.timeout)
	if err := dl.RecursiveUpload(childCtx, outputInterface, u.localDirectoryPath, storage.DataReference(u.remoteOutputsPrefix), storage.DataReference(u.remoteOutputsSandbox)); err != nil {
		logger.Errorf(ctx, "Uploading failed, err %s", err)
		return err
	}

	logger.Infof(ctx, "Uploader completed successfully!")
	return nil
}

func (u *UploadOptions) Upload(ctx context.Context) error {

	if err := u.uploader(ctx); err != nil {
		logger.Errorf(ctx, "Uploading failed, err %s", err)
		if err := u.UploadError(ctx, "OutputUploadFailed", err, storage.DataReference(u.remoteOutputsPrefix)); err != nil {
			logger.Errorf(ctx, "Failed to write error document, err :%s", err)
			return err
		}
	}
	return nil
}

func NewUploadCommand(opts *RootOptions) *cobra.Command {

	uploadOptions := &UploadOptions{
		RootOptions: opts,
	}

	// deleteCmd represents the delete command
	uploadCmd := &cobra.Command{
		Use:   "upload <opts>",
		Short: "uploads flytedata from the localpath to a remote dir.",
		Long:  `Currently it looks at the outputs.pb and creates one file per variable.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return uploadOptions.Upload(context.Background())
		},
	}

	uploadCmd.Flags().StringVarP(&uploadOptions.remoteOutputsPrefix, "to-output-prefix", "p", "", "The remote path/key prefix for output metadata in stow store.")
	uploadCmd.Flags().StringVarP(&uploadOptions.remoteOutputsSandbox, "to-sandbox", "x", "", "The remote path/key prefix for outputs in stow store. This is a sandbox directory and all data will be uploaded here.")
	uploadCmd.Flags().StringVarP(&uploadOptions.localDirectoryPath, "from-local-dir", "d", "", "The local directory on disk where data will be available for upload.")
	uploadCmd.Flags().StringVarP(&uploadOptions.outputFormat, "format", "m", "json", fmt.Sprintf("What should be the output format for the primitive and structured types. Options [%v]", data.AllOutputFormats))
	uploadCmd.Flags().DurationVarP(&uploadOptions.timeout, "timeout", "t", time.Hour*1, "Max time to allow for uploads to complete, default is 1H")
	uploadCmd.Flags().BytesBase64VarP(&uploadOptions.outputInterface, "output-interface", "i", nil, "Output interface proto message - core.VariableMap, base64 encoced string")
	uploadCmd.Flags().DurationVarP(&uploadOptions.containerStartTimeout, "start-timeout", "u", 0, "Max time to allow for container to startup. 0 indicates wait for ever.")
	uploadCmd.Flags().StringVarP(&uploadOptions.startWatcherType, "start-watcher-type", "w", containerwatcher.WatcherTypeSharedProcessNS, fmt.Sprintf("Upload will wait for container before starting upload process. Watcher type makes the type configurable. Avaialble Type %+v", containerwatcher.AllWatcherTypes))
	uploadCmd.Flags().StringVarP(&uploadOptions.exitWatcherType, "exit-watcher-type", "k", containerwatcher.WatcherTypeSharedProcessNS, fmt.Sprintf("Upload will wait for completion of the container before starting upload process. Watcher type makes the type configurable. Avaialble Type %+v", containerwatcher.AllWatcherTypes))
	uploadCmd.Flags().StringVarP(&uploadOptions.containerInfo.Name, "watch-container", "c", "", "For KubeAPI watcher, Wait for this container to exit.")
	uploadCmd.Flags().StringVarP(&uploadOptions.containerInfo.Namespace, "namespace", "n", "", "For KubeAPI watcher, Namespace of the pod [optional]")
	uploadCmd.Flags().StringVarP(&uploadOptions.containerInfo.Name, "pod-name", "o", "", "For KubeAPI watcher, Name of the pod [optional].")
	return uploadCmd
}
