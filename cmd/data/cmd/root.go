package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/config/viper"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/storage"
	"github.com/lyft/flytestdlib/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type RootOptions struct {
	showSource bool
	Scope      promutils.Scope
	Store      *storage.DataStore
}

func (r *RootOptions) executeRootCmd() error {
	ctx := context.TODO()
	logger.Infof(ctx, "Go Version: %s", runtime.Version())
	logger.Infof(ctx, "Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
	version.LogBuildInformation("flytedata")
	return fmt.Errorf("use one of the sub-commands")
}

func (r RootOptions) UploadError(ctx context.Context, code string, recvErr error, prefix storage.DataReference) error {
	if recvErr == nil {
		recvErr = fmt.Errorf("unknown error")
	}
	errorPath, err := r.Store.ConstructReference(ctx, prefix, "errors.pb")
	if err != nil {
		logger.Errorf(ctx, "failed to create error file path err: %s", err)
		return err
	}
	return r.Store.WriteProtobuf(ctx, errorPath, storage.Options{}, &core.ErrorDocument{
		Error: &core.ContainerError{
			Code:    code,
			Message: recvErr.Error(),
			Kind:    core.ContainerError_RECOVERABLE,
		},
	})
}

// NewCommand returns a new instance of an argo command
func NewDataCommand() *cobra.Command {
	rootOpts := &RootOptions{}
	command := &cobra.Command{
		Use:   "flytedata",
		Short: "flytedata is a simple go binary that can be used to retrieve and upload data from/to remote stow store to local disk.",
		Long:  `flytedata when used with conjunction with flytepropeller eliminates the need to have any flyte library installed inside the container`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return rootOpts.executeRootCmd()
		},
	}

	command.AddCommand(NewDownloadCommand(rootOpts))
	command.AddCommand(NewUploadCommand(rootOpts))

	command.PersistentFlags().BoolVarP(&rootOpts.showSource, "show-source", "s", false, "Show line number for errors")
	command.AddCommand(viper.GetConfigCommand())

	return command
}

func init() {
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		logger.Error(context.TODO(), "Error in initializing: %v", err)
		os.Exit(-1)
	}
}
