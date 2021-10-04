package cmd

import (
	"context"
	"fmt"
	//"errors"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	//"github.com/flyteorg/flytepropeller/pkg/manager"
	managerConfig "github.com/flyteorg/flytepropeller/pkg/manager/config"
	"github.com/flyteorg/flytepropeller/pkg/signals"

	"github.com/flyteorg/flytestdlib/logger"
	//"github.com/flyteorg/flytestdlib/profutils"
	//"github.com/flyteorg/flytestdlib/promutils"

	"github.com/spf13/cobra"

	//"k8s.io/client-go/kubernetes"
	//apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

var managerCmd = &cobra.Command{
	Use:     "manager",
	Aliases: []string{"managers"},
	Short:   "TODO",
	Long: `
	TODO
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runManager(context.Background(), config.GetConfig(), managerConfig.GetConfig())
	},
}

func init() {
	rootCmd.AddCommand(managerCmd)
}

func runManager(origContext context.Context, propellerCfg *config.Config, cfg *managerConfig.Config) error {
	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalHandler(origContext)

	kubeClient, _, err := getKubeConfig(ctx, propellerCfg)
	if err != nil {
		return err
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

	// TODO hamersaw - use [ListOptions](https://pkg.go.dev/k8s.io/apimachinery@v0.20.2/pkg/apis/meta/v1#ListOptions)
	labelMap := map[string]string{
		"app": "flytepropeller",
	}
	options := metav1.ListOptions{
        LabelSelector: labels.SelectorFromSet(labelMap).String(),
    }

	ticker := time.NewTicker(2 * time.Second)
	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				pods, err := kubeClient.CoreV1().Pods("flyte").List(ctx, options)
				if err != nil {
					panic(err.Error())
				}
				fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))
			}
		}
	}()

	logger.Info(ctx, "Started manager")
	<-ctx.Done()

	logger.Info(ctx, "Shutting down manager")
	done <- true

	/*limitNamespace := ""
	if propellerCfg.LimitNamespace != defaultNamespace {
		limitNamespace = propellerCfg.LimitNamespace
	}*/

	/*// Creates a MutationConfig to instruct ApiServer to call this service whenever a Pod is being created.
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
	}

	err = secretsManager.Register(ctx, mgr)
	if err != nil {
		logger.Fatalf(ctx, "Failed to register manager with manager. Error: %v", err)
	}*/

	//logger.Infof(ctx, "Starting controller-runtime webhook")
	//return mgr.Start(ctx)

	return nil
}
