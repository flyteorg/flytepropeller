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
	"fmt"

	"github.com/spf13/cobra"
)

type ExplodeType = string

const (
	// will convert flyte proto to one file per variable, the output format will match as specified
	ExplodeTypeOnePerVar ExplodeType = "one-per-var"
	// Will download contents to one file in the output format specified. For Blob, Multipart-Blobs, Schemas and other large datatypes
	// A reference to a local file path will be stored in this file
	ExplodeTypeSingleFile ExplodeType = "single-file-json"
)

var explodeTypes = []ExplodeType{
	ExplodeTypeOnePerVar,
	ExplodeTypeSingleFile,
}

type OutputFormat = string

const (
	OutputFormatJSON OutputFormat = "json"
	OutputFormatYAML OutputFormat = "yaml"
)

var outputFormats = []OutputFormat{
	OutputFormatJSON,
	OutputFormatYAML,
}

type DownloadOptions struct {
	*RootOptions
	remotePath         string
	localDirectoryPath string
	// Non primitive types will be dumped in this output format
	outputFormat OutputFormat
	// Directive on how should the data be exploded into the local path
	explodeType ExplodeType
}

func (d *DownloadOptions) Download() error {
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
			return downloadOpts.Download()
		},
	}

	downloadCmd.Flags().StringVarP(&downloadOpts.remotePath, "from-remote", "f", "", "The remote path/key for stow store.")
	downloadCmd.Flags().StringVarP(&downloadOpts.localDirectoryPath, "to-local", "t", "", "The local directory on disk where data should be downloaded.")
	downloadCmd.Flags().StringVarP(&downloadOpts.explodeType, "explode", "x", "one-per-var", fmt.Sprintf("How to explode the input data. Options [%v]", explodeTypes))
	downloadCmd.Flags().StringVarP(&downloadOpts.outputFormat, "format", "m", "json", fmt.Sprintf("What should be the output format for the primitive and structured types. Options [%v]", outputFormats))

	return downloadCmd
}
