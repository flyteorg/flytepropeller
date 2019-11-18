// Copyright © 2019 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
}

func (u *UploadOptions) Upload(ctx context.Context) error {

	outputInterface := &core.VariableMap{}
	if err := proto.Unmarshal(u.outputInterface, outputInterface); err != nil {
		logger.Errorf(ctx, "Bad output interface passed, failed to unmarshal err :%s", err)
		return errors.Wrap(err, "Bad output interface passed, failed to unmarshal")
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
	return uploadCmd
}
