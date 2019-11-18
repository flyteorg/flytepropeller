package cmd

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/golang/protobuf/proto"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/lyft/flytepropeller/data"
)

type UploadOptions struct {
	*RootOptions
	remoteOutputsPrefix string
	localDirectoryPath  string
	// Non primitive types will be dumped in this output format
	outputFormat    data.Format
	timeout         time.Duration
	outputInterface []byte
	useSuccessFile  bool
}

func WaitForSuccessFileToExist(path string) error {
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
	if err := w.Add(path); err != nil {
		return err
	}
	return <-done
}

func (u *UploadOptions) Upload(ctx context.Context) error {

	if u.outputInterface == nil {
		return fmt.Errorf("output interface is required")
	}

	outputInterface := &core.VariableMap{}
	if err := proto.Unmarshal(u.outputInterface, outputInterface); err != nil {
		logger.Errorf(ctx, "Bad output interface passed, failed to unmarshal err :%s", err)
		return errors.Wrap(err, "Bad output interface passed, failed to unmarshal")
	}

	if u.useSuccessFile {
		if err := WaitForSuccessFileToExist(path.Join(u.localDirectoryPath, "_SUCCESS")); err != nil {
			return err
		}
	}

	dl := data.NewUploader(ctx, u.Store, u.outputFormat)
	childCtx, _ := context.WithTimeout(ctx, u.timeout)
	err := dl.RecursiveUpload(childCtx, outputInterface, u.localDirectoryPath, storage.DataReference(u.remoteOutputsPrefix))
	if err != nil {
		logger.Errorf(ctx, "Uploading failed, err %s", err)
		if err := u.UploadError(ctx, "OutputUploadFailed", err, storage.DataReference(u.remoteOutputsPrefix)); err != nil {
			logger.Errorf(ctx, "Failed to write error document, err :%s", err)
		}
		return err
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

	uploadCmd.Flags().StringVarP(&uploadOptions.remoteOutputsPrefix, "to-remote-prefix", "p", "", "The remote path/key prefix for outputs in stow store. this is mostly used to write errors.pb.")
	uploadCmd.Flags().StringVarP(&uploadOptions.localDirectoryPath, "to-local-dir", "d", "", "The local directory on disk where data should be downloaded.")
	uploadCmd.Flags().StringVarP(&uploadOptions.outputFormat, "format", "m", "json", fmt.Sprintf("What should be the output format for the primitive and structured types. Options [%v]", data.AllOutputFormats))
	uploadCmd.Flags().DurationVarP(&uploadOptions.timeout, "timeout", "t", time.Hour*1, "Max time to allow for downloads to complete, default is 1H")
	uploadCmd.Flags().BytesBase64VarP(&uploadOptions.outputInterface, "output-interface", "i", nil, "Output interface proto message - core.VariableMap, base64 encoced string")
	uploadCmd.Flags().BoolVarP(&uploadOptions.useSuccessFile, "use-success-file", "s", true, "Upload will wait for a success file to be written before starting upload process.")
	return uploadCmd
}
