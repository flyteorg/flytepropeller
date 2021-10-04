package cmd

import (
	"context"
	//"encoding/json"
	"fmt"
	//"errors"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	//"github.com/flyteorg/flytepropeller/pkg/manager"
	managerConfig "github.com/flyteorg/flytepropeller/pkg/manager/config"
	"github.com/flyteorg/flytepropeller/pkg/signals"

	//"github.com/flyteorg/flytestdlib/logger"
	//"github.com/flyteorg/flytestdlib/profutils"
	//"github.com/flyteorg/flytestdlib/promutils"

	"github.com/spf13/cobra"

	//"k8s.io/client-go/kubernetes"
	//apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	/*raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}*/

	kubeClient, _, err := getKubeConfig(ctx, propellerCfg)
	if err != nil {
		return err
	}

	// TODO hamersaw - use [ListOptions](https://pkg.go.dev/k8s.io/apimachinery@v0.20.2/pkg/apis/meta/v1#ListOptions)
	for {
		pods, err := kubeClient.CoreV1().Pods("flyte").List(ctx, metav1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))
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
