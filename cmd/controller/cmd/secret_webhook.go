package cmd

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/signals"
	"github.com/flyteorg/flytepropeller/pkg/webhook"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/profutils"
	"github.com/flyteorg/flytestdlib/promutils"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/spf13/cobra"
)

var webhookCmd = &cobra.Command{
	Use: "webhook",
}

var secretsWebhook = &cobra.Command{
	Use: "secrets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSecretsWebhook(context.Background(), config.GetConfig())
	},
}

var allWebhooks = &cobra.Command{
	Use: "all",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSecretsWebhook(context.Background(), config.GetConfig())
	},
}

func init() {
	webhookCmd.AddCommand(allWebhooks)
	webhookCmd.AddCommand(secretsWebhook)
	rootCmd.AddCommand(webhookCmd)
}

func runSecretsWebhook(ctx context.Context, cfg *config.Config) error {
	// set up signals so we handle the first shutdown signal gracefully
	ctx = signals.SetupSignalHandler(ctx)

	_, kubecfg, err := getKubeConfig(ctx, cfg)
	if err != nil {
		logger.Fatalf(ctx, "Error building kubernetes clientset: %s", err.Error())
	}

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
		Namespace:     limitNamespace,
		SyncPeriod:    &cfg.DownstreamEval.Duration,
		ClientBuilder: executors.NewFallbackClientBuilder(),
	})
	if err != nil {
		logger.Fatalf(ctx, "Failed to initialize controller run-time manager. Error: %v", err)
	}

	secretsWebhook := webhook.NewSecretsWebhook(propellerScope)
	err = secretsWebhook.Register(ctx, mgr)
	if err != nil {
		logger.Fatalf(ctx, "Failed to register webhook with manager. Error: %v", err)
	}

	logger.Infof(ctx, "Starting controller-runtime manager")
	return mgr.Start(ctx)
}
