// Package cmd contains commands for FlytePropeller controller.
package cmd

import (
	"context"
	"flag"
	"github.com/flyteorg/flytestdlib/profutils"
	"k8s.io/klog"
	"os"
	"runtime"

	config2 "github.com/flyteorg/flytepropeller/pkg/controller/config"

	"github.com/flyteorg/flytestdlib/config/viper"
	"github.com/flyteorg/flytestdlib/version"

	"github.com/flyteorg/flytestdlib/config"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/spf13/pflag"

	"github.com/spf13/cobra"

	"github.com/flyteorg/flytepropeller/pkg/controller"
	"github.com/flyteorg/flytepropeller/pkg/signals"
)

const (
	defaultNamespace = "all"
	appName          = "flytepropeller"
)

var (
	cfgFile        string
	configAccessor = viper.NewAccessor(config.Options{StrictMode: true})
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "flyte-propeller",
	Short: "Operator for running Flyte Workflows",
	Long: `Flyte Propeller runs a workflow to completion by recursing through the nodes, 
			handling their tasks to completion and propagating their status upstream.`,
	PersistentPreRunE: initConfig,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		return executeRootCmd(ctx, config2.GetConfig())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	version.LogBuildInformation(appName)
	logger.Infof(context.TODO(), "Detected: %d CPU's\n", runtime.NumCPU())
	if err := rootCmd.Execute(); err != nil {
		logger.Error(context.TODO(), err)
		os.Exit(1)
	}
}

func init() {
	// allows `$ flytepropeller --logtostderr` to work
	klog.InitFlags(flag.CommandLine)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		logAndExit(err)
	}

	// Here you will define your flags and configuration settings. Cobra supports persistent flags, which, if defined
	// here, will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file (default is $HOME/config.yaml)")

	configAccessor.InitializePflags(rootCmd.PersistentFlags())

	rootCmd.AddCommand(viper.GetConfigCommand())
}

func initConfig(cmd *cobra.Command, _ []string) error {
	configAccessor = viper.NewAccessor(config.Options{
		StrictMode:  false,
		SearchPaths: []string{cfgFile},
	})

	configAccessor.InitializePflags(cmd.PersistentFlags())

	err := configAccessor.UpdateConfig(context.TODO())
	if err != nil {
		return err
	}

	return nil
}

func logAndExit(err error) {
	logger.Error(context.Background(), err)
	os.Exit(-1)
}

func executeRootCmd(baseCtx context.Context, cfg *config2.Config) error {
	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalHandler(baseCtx)

	go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, cfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Fatalf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()

	return controller.StartController(ctx, cfg, defaultNamespace)
}
