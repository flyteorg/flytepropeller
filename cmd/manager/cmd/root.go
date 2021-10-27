// Commands for FlytePropeller manager.
package cmd

import (
	"context"
	"flag"
	"os"
	"runtime"
	//"runtime/pprof" // TODO hamersaw - add pprof

	"github.com/flyteorg/flytestdlib/config"
	"github.com/flyteorg/flytestdlib/config/viper"
	"github.com/flyteorg/flytestdlib/logger"
	//"github.com/flyteorg/flytestdlib/profutils" // TODO hamersaw - enable
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/version"

	"github.com/flyteorg/flytepropeller/manager"
	managerConfig "github.com/flyteorg/flytepropeller/manager/config"
	propellerConfig "github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/signals"
	"github.com/flyteorg/flytepropeller/pkg/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/klog"
)

const (
	appName = "flytepropeller-manager"
)

var (
	cfgFile        string
	configAccessor = viper.NewAccessor(config.Options{StrictMode: true})
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   appName,
	Short: "TODO",
	Long:  `TODO`,
	PersistentPreRunE: initConfig,
	Run: func(cmd *cobra.Command, args []string) {
		executeRootCmd(propellerConfig.GetConfig(), managerConfig.GetConfig())
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

func executeRootCmd(propellerCfg *propellerConfig.Config, cfg *managerConfig.Config) {
	baseCtx := context.Background()

	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalHandler(baseCtx)

	kubeClient, _, err := utils.GetKubeConfig(ctx, propellerCfg)
	if err != nil {
		logger.Fatalf(ctx, "error building kubernetes clientset: %s", err.Error())
	}

	// Add the propeller_manager subscope because the MetricsPrefix only has "flyte:" to get uniform collection of metrics.
	scope := promutils.NewScope(propellerCfg.MetricsPrefix).NewSubScope("propeller_manager")

	// TODO hamersaw - reenable for metrics
	/*go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, propellerCfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Panicf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()*/

	m, err := manager.New(ctx, cfg, kubeClient, scope)
	if err != nil {
		logger.Fatalf(ctx, "failed to start Manager - [%v]", err.Error())
	} else if m == nil {
		logger.Fatalf(ctx, "failed to start Manager, nil manager received.")
	}

	if err = m.Run(ctx); err != nil {
		logger.Fatalf(ctx, "error running Manager: %s", err.Error())
	}
}
