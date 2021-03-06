package cmd

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/spf13/cobra"
)

var webhook = &cobra.Command{
	Use: "webhook",
}

var secretsWebhook = &cobra.Command{
	Use: "secrets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return runSecretsWebhook(context.Background(), config.GetConfig())
	},
}

func init() {
	webhook.AddCommand(secretsWebhook)
	rootCmd.AddCommand(webhook)
}

func runSecretsWebhook(ctx context.Context, cfg *config.Config) error {
	return nil
}
