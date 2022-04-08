package cmd

import (
	"context"

	webhookConfig "github.com/flyteorg/flytepropeller/pkg/webhook/config"
	"github.com/flyteorg/flytestdlib/profutils"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/signals"
	"github.com/flyteorg/flytepropeller/pkg/webhook"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/spf13/cobra"
)

var webhookCmd = &cobra.Command{
	Use:     "webhook",
	Aliases: []string{"webhooks"},
	Short:   "Runs Propeller Pod Webhook that listens for certain labels and modify the pod accordingly.",
	Long: `
This command initializes propeller's Pod webhook that enables it to mutate pods whether they are created directly from
plugins or indirectly through the creation of other CRDs (e.g. Spark/Pytorch). 
In order to use this Webhook:
1) Keys need to be mounted to the POD that runs this command; tls.crt should be a CA-issued cert (not a self-signed 
   cert), tls.key as the private key for that cert and, optionally, ca.crt in case tls.crt's CA is not a known 
   Certificate Authority (e.g. in case ca.crt is self-issued).
2) POD_NAME and POD_NAMESPACE environment variables need to be populated because the webhook initialization will lookup
   this pod to copy OwnerReferences into the new MutatingWebhookConfiguration object it'll create to ensure proper
   cleanup.

A sample Container for this webhook might look like this:

      volumes:
        - name: config-volume
          configMap:
            name: flyte-propeller-config-492gkfhbgk
        # Certs secret created by running 'flytepropeller webhook init-certs' 
        - name: webhook-certs
          secret:
            secretName: flyte-pod-webhook
      containers:
        - name: webhook-server
          image: <image>
          command:
            - flytepropeller
          args:
            - webhook
            - --config
            - /etc/flyte/config/*.yaml
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          volumeMounts:
            - name: config-volume
              mountPath: /etc/flyte/config
              readOnly: true
            # Mount certs from a secret
            - name: webhook-certs
              mountPath: /etc/webhook/certs
              readOnly: true
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		return runWebhook(context.Background(), config.GetConfig(), webhookConfig.GetConfig())
	},
}

func init() {
	rootCmd.AddCommand(webhookCmd)
}

func runWebhook(origContext context.Context, propellerCfg *config.Config, cfg *webhookConfig.Config) error {
	// set up signals so we handle the first shutdown signal gracefully
	ctx := signals.SetupSignalHandler(origContext)

	go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, propellerCfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Panicf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()
	return webhook.Run(ctx, propellerCfg, cfg, defaultNamespace)
}
