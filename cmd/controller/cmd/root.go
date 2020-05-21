package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"k8s.io/klog"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/lyft/flytepropeller/pkg/controller/executors"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/controller-runtime/pkg/manager"

	config2 "github.com/lyft/flytepropeller/pkg/controller/config"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytestdlib/config/viper"
	"github.com/lyft/flytestdlib/version"

	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/profutils"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/spf13/cobra"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	restclient "k8s.io/client-go/rest"

	clientset "github.com/lyft/flytepropeller/pkg/client/clientset/versioned"
	informers "github.com/lyft/flytepropeller/pkg/client/informers/externalversions"
	"github.com/lyft/flytepropeller/pkg/controller"
	"github.com/lyft/flytepropeller/pkg/signals"
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
	PreRunE: initConfig,
	Run: func(cmd *cobra.Command, args []string) {
		executeRootCmd(config2.GetConfig())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	version.LogBuildInformation(appName)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
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

func initConfig(_ *cobra.Command, _ []string) error {
	configAccessor = viper.NewAccessor(config.Options{
		StrictMode:  true,
		SearchPaths: []string{cfgFile},
	})

	err := configAccessor.UpdateConfig(context.TODO())
	if err != nil {
		return err
	}

	fmt.Printf("Started in-cluster mode\n")
	return nil
}

func logAndExit(err error) {
	logger.Error(context.Background(), err)
	os.Exit(-1)
}

func getKubeConfig(_ context.Context, cfg *config2.Config) (*kubernetes.Clientset, *restclient.Config, error) {
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

func sharedInformerOptions(cfg *config2.Config) []informers.SharedInformerOption {
	opts := []informers.SharedInformerOption{
		informers.WithTweakListOptions(func(options *v1.ListOptions) {
			options.LabelSelector = v1.FormatLabelSelector(controller.IgnoreCompletedWorkflowsLabelSelector())
		}),
	}
	if cfg.LimitNamespace != defaultNamespace {
		opts = append(opts, informers.WithNamespace(cfg.LimitNamespace))
	}
	return opts
}

func safeMetricName(original string) string {
	// TODO: Replace all non-prom-compatible charset
	return strings.Replace(original, "-", "_", -1)
}

func executeRootCmd(cfg *config2.Config) {
	baseCtx := context.Background()

	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalHandler(baseCtx)

	kubeClient, kubecfg, err := getKubeConfig(ctx, cfg)
	if err != nil {
		logger.Fatalf(ctx, "Error building kubernetes clientset: %s", err.Error())
	}

	flyteworkflowClient, err := clientset.NewForConfig(kubecfg)
	if err != nil {
		logger.Fatalf(ctx, "Error building example clientset: %s", err.Error())
	}

	opts := sharedInformerOptions(cfg)
	flyteworkflowInformerFactory := informers.NewSharedInformerFactoryWithOptions(flyteworkflowClient, cfg.WorkflowReEval.Duration, opts...)

	// Add the propeller subscope because the MetricsPrefix only has "flyte:" to get uniform collection of metrics.
	propellerScope := promutils.NewScope(cfg.MetricsPrefix).NewSubScope("propeller").NewSubScope(safeMetricName(cfg.LimitNamespace))

	go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, cfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Panicf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()

	limitNamespace := ""
	if cfg.LimitNamespace != defaultNamespace {
		limitNamespace = cfg.LimitNamespace
	}

	mgr, err := manager.New(kubecfg, manager.Options{
		Namespace:  limitNamespace,
		SyncPeriod: &cfg.DownstreamEval.Duration,
		NewClient: func(cache cache.Cache, config *restclient.Config, options client.Options) (i client.Client, e error) {
			rawClient, err := client.New(kubecfg, client.Options{})
			if err != nil {
				return nil, err
			}

			return executors.NewFallbackClient(&client.DelegatingClient{
				Reader: &client.DelegatingReader{
					CacheReader:  cache,
					ClientReader: rawClient,
				},
				Writer:       rawClient,
				StatusClient: rawClient,
			}, rawClient), nil
		},
	})
	if err != nil {
		logger.Fatalf(ctx, "Failed to initialize controller run-time manager. Error: %v", err)
	}

	go func() {
		err = mgr.Start(ctx.Done())
		if err != nil {
			logger.Fatalf(ctx, "Failed to start manager. Error: %v", err)
		}
	}()

	c, err := controller.New(ctx, cfg, kubeClient, flyteworkflowClient, flyteworkflowInformerFactory, mgr, propellerScope)

	if err != nil {
		logger.Fatalf(ctx, "Failed to start Controller - [%v]", err.Error())
	} else if c == nil {
		logger.Fatalf(ctx, "Failed to start Controller, nil controller received.")
	}

	go flyteworkflowInformerFactory.Start(ctx.Done())

	if err = c.Run(ctx); err != nil {
		logger.Fatalf(ctx, "Error running controller: %s", err.Error())
	}
}
