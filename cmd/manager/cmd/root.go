// Commands for FlytePropeller manager.
package cmd

import (
	"context"
	"flag"
	"os"
	"runtime"
	//"runtime/pprof" // TODO hamersaw - add pprof
	"strings"

	"github.com/flyteorg/flytestdlib/config"
	"github.com/flyteorg/flytestdlib/config/viper"
	"github.com/flyteorg/flytestdlib/logger"
	// TODO hamersaw - enable
	//"github.com/flyteorg/flytestdlib/profutils"
	//"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/version"

	"github.com/flyteorg/flytepropeller/manager"
	managerConfig "github.com/flyteorg/flytepropeller/manager/config"
	propellerConfig "github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/signals"

	"github.com/pkg/errors"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
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
	Long: `TODO`,
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

func getKubeConfig(_ context.Context, cfg *propellerConfig.Config) (*kubernetes.Clientset, *restclient.Config, error) {
	var kubecfg *restclient.Config
	var err error
	if cfg.KubeConfigPath != "" {
		kubeConfigPath := os.ExpandEnv(cfg.KubeConfigPath)
		kubecfg, err = clientcmd.BuildConfigFromFlags(cfg.MasterURL, kubeConfigPath)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "Error building kubeconfig")
		}
	} else {
		kubecfg, err = restclient.InClusterConfig()
		if err != nil {
			return nil, nil, errors.Wrapf(err, "Cannot get InCluster kubeconfig")
		}
	}

	kubecfg.QPS = cfg.KubeConfig.QPS
	kubecfg.Burst = cfg.KubeConfig.Burst
	kubecfg.Timeout = cfg.KubeConfig.Timeout.Duration

	kubeClient, err := kubernetes.NewForConfig(kubecfg)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Error building kubernetes clientset")
	}
	return kubeClient, kubecfg, err
}

func safeMetricName(original string) string {
	// TODO: Replace all non-prom-compatible charset
	return strings.Replace(original, "-", "_", -1)
}

func executeRootCmd(propellerCfg *propellerConfig.Config, cfg *managerConfig.Config) {
	baseCtx := context.Background()

	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalHandler(baseCtx)

	kubeClient, _, err := getKubeConfig(ctx, propellerCfg)
	if err != nil {
		logger.Fatalf(ctx, "Error building kubernetes clientset: %s", err.Error())
	}

	// TODO hamersaw - reenable for metrics
	/*// Add the propeller subscope because the MetricsPrefix only has "flyte:" to get uniform collection of metrics.
	propellerScope := promutils.NewScope(cfg.MetricsPrefix).NewSubScope("propeller").NewSubScope(safeMetricName(propellerCfg.LimitNamespace))

	go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, propellerCfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Panicf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()*/


	/*limitNamespace := ""
	if propellerCfg.LimitNamespace != defaultNamespace {
		limitNamespace = propellerCfg.LimitNamespace
	}

	// Creates a MutationConfig to instruct ApiServer to call this service whenever a Pod is being created.
	err = createMutationConfig(ctx, kubeClient, secretsManager)
	if err != nil {
		return err
	}

	mgr, err := manager.New(kubecfg, manager.Options{
		Port:          cfg.ListenPort,
		CertDir:       cfg.CertDir,
		Namespace:     limitNamespace,
		SyncPeriod:    &propellerCfg.DownstreamEval.Duration,
		ClientBuilder: executors.NewFallbackClientBuilder(),
	})

	if err != nil {
		logger.Fatalf(ctx, "Failed to initialize controller run-time manager. Error: %v", err)
	}*/

	m, err := manager.New(ctx, cfg, kubeClient)
	if err != nil {
		logger.Fatalf(ctx, "Failed to start Manager - [%v]", err.Error())
	} else if m == nil {
		logger.Fatalf(ctx, "Failed to start Manager, nil manager received.")
	}

	if err = m.Run(ctx); err != nil {
		logger.Fatalf(ctx, "Error running Manager: %s", err.Error())
	}
}
