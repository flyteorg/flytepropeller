package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/lyft/flytestdlib/config/viper"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "data",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

type RootOptions struct {
	showSource bool
}

func (r *RootOptions) executeRootCmd() error {
	ctx := context.TODO()
	logger.Infof(ctx, "Go Version: %s", runtime.Version())
	logger.Infof(ctx, "Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
	version.LogBuildInformation("flytedata")
	return fmt.Errorf("use one of the sub-commands")
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
