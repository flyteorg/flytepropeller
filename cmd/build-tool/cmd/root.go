package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/version"
	"github.com/spf13/pflag"

	"github.com/spf13/cobra"
)

func init() {
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		logger.Error(context.TODO(), "Error in initializing: %v", err)
		os.Exit(-1)
	}
}

type RootOptions struct{}

func (r *RootOptions) executeRootCmd() error {
	ctx := context.TODO()
	logger.Infof(ctx, "Go Version: %s", runtime.Version())
	logger.Infof(ctx, "Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
	version.LogBuildInformation("build-tool")
	return fmt.Errorf("use one of the sub-commands")
}

// NewCommand returns a new instance of an argo command
func NewBuildToolCommand() *cobra.Command {
	rootOpts := &RootOptions{}
	command := &cobra.Command{
		Use:   "build-tool",
		Short: "build-tool are utility commands that help validating crds, etc.",
		Long: `Flyte is a serverless workflow processing platform built for native execution on K8s.
      It is extensible and flexible to allow adding new operators and comes with many operators built in`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return rootOpts.executeRootCmd()
		},
	}

	command.AddCommand(NewCrdValidationCommand(rootOpts))
	return command
}
