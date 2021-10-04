package cmd

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/manager"
	managerConfig "github.com/flyteorg/flytepropeller/pkg/manager/config"
	"github.com/flyteorg/flytepropeller/pkg/signals"

	"github.com/flyteorg/flytestdlib/logger"
	//"github.com/flyteorg/flytestdlib/profutils"
	//"github.com/flyteorg/flytestdlib/promutils"

	"github.com/spf13/cobra"
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

	return nil
}
