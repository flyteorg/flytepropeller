package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/spf13/cobra"

	"github.com/lyft/flytepropeller/data"
)

type DownloadOptions struct {
	*RootOptions
	remoteInputsPath    string
	remoteOutputsPrefix string
	localDirectoryPath  string
	// Non primitive types will be dumped in this output format
	outputFormat data.Format
	timeout      time.Duration
}

func (d *DownloadOptions) Download(ctx context.Context) error {
	dl := data.NewDownloader(ctx, d.Store, d.outputFormat)
	childCtx, _ := context.WithTimeout(ctx, d.timeout)
	err := dl.DownloadInputs(childCtx, storage.DataReference(d.remoteInputsPath), d.localDirectoryPath)
	if err != nil {
		logger.Errorf(ctx, "Downloading failed, err %s", err)
		if err := d.UploadError(ctx, "InputDownloadFailed", err, storage.DataReference(d.remoteOutputsPrefix)); err != nil {
			logger.Errorf(ctx, "Failed to write error document, err :%s", err)
		}
		return err
	}
	return nil
}

func NewDownloadCommand(opts *RootOptions) *cobra.Command {

	downloadOpts := &DownloadOptions{
		RootOptions: opts,
	}

	// deleteCmd represents the delete command
	downloadCmd := &cobra.Command{
		Use:   "download <opts>",
		Short: "downloads flytedata from the remotepath to a local directory.",
		Long:  `Currently it looks at the outputs.pb and creates one file per variable.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return downloadOpts.Download(context.Background())
		},
	}

	downloadCmd.Flags().StringVarP(&downloadOpts.remoteInputsPath, "from-remote", "f", "", "The remote path/key for inputs in stow store.")
	downloadCmd.Flags().StringVarP(&downloadOpts.remoteOutputsPrefix, "to-remote-prefix", "p", "", "The remote path/key prefix for outputs in stow store. this is mostly used to write errors.pb.")
	downloadCmd.Flags().StringVarP(&downloadOpts.localDirectoryPath, "to-local-dir", "d", "", "The local directory on disk where data should be downloaded.")
	downloadCmd.Flags().StringVarP(&downloadOpts.outputFormat, "format", "m", "json", fmt.Sprintf("What should be the output format for the primitive and structured types. Options [%v]", data.AllOutputFormats))
	downloadCmd.Flags().DurationVarP(&downloadOpts.timeout, "timeout", "t", time.Hour*1, "Max time to allow for downloads to complete, default is 1H")
	return downloadCmd
}
