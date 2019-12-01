package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/config/viper"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/lyft/flytestdlib/storage"
	"github.com/lyft/flytestdlib/version"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
)

type RootOptions struct {
	*clientcmd.ConfigOverrides
	allNamespaces  bool
	showSource     bool
	clientConfig   clientcmd.ClientConfig
	restConfig     *rest.Config
	kubeClient     kubernetes.Interface
	Scope          promutils.Scope
	Store          *storage.DataStore
	configAccessor config.Accessor
	cfgFile        string
}

func (r *RootOptions) executeRootCmd() error {
	ctx := context.TODO()
	logger.Infof(ctx, "Go Version: %s", runtime.Version())
	logger.Infof(ctx, "Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
	version.LogBuildInformation("flytedata")
	return fmt.Errorf("use one of the sub-commands")
}

func (r RootOptions) UploadError(ctx context.Context, code string, recvErr error, prefix storage.DataReference) error {
	logger.Infof(ctx, "Uploading Error file")
	if recvErr == nil {
		recvErr = fmt.Errorf("unknown error")
	}
	errorPath, err := r.Store.ConstructReference(ctx, prefix, "errors.pb")
	if err != nil {
		logger.Errorf(ctx, "failed to create error file path err: %s", err)
		return err
	}
	logger.Infof(ctx, "Uploading Error file to path [%s]", errorPath)
	return r.Store.WriteProtobuf(ctx, errorPath, storage.Options{}, &core.ErrorDocument{
		Error: &core.ContainerError{
			Code:    code,
			Message: recvErr.Error(),
			Kind:    core.ContainerError_RECOVERABLE,
		},
	})
}

// NewCommand returns a new instance of an argo command
func NewDataCommand() *cobra.Command {
	rootOpts := &RootOptions{}
	command := &cobra.Command{
		Use:   "flytedata",
		Short: "flytedata is a simple go binary that can be used to retrieve and upload data from/to remote stow store to local disk.",
		Long:  `flytedata when used with conjunction with flytepropeller eliminates the need to have any flyte library installed inside the container`,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := rootOpts.initConfig(cmd, args); err != nil {
				return err
			}
			rootOpts.Scope = promutils.NewScope("flyte:data")
			cfg := storage.GetConfig()
			store, err := storage.NewDataStore(cfg, rootOpts.Scope)
			if err != nil {
				return errors.Wrap(err, "failed to create datastore client")
			}
			rootOpts.Store = store
			return rootOpts.ConfigureClient()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return rootOpts.executeRootCmd()
		},
	}

	command.AddCommand(NewDownloadCommand(rootOpts))
	command.AddCommand(NewUploadCommand(rootOpts))

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.DefaultClientConfig = &clientcmd.DefaultClientConfig
	rootOpts.ConfigOverrides = &clientcmd.ConfigOverrides{}
	kflags := clientcmd.RecommendedConfigOverrideFlags("")
	command.PersistentFlags().StringVar(&loadingRules.ExplicitPath, "kubeconfig", "", "Path to a kube config. Only required if out-of-cluster")
	clientcmd.BindOverrideFlags(rootOpts.ConfigOverrides, command.PersistentFlags(), kflags)
	rootOpts.clientConfig = clientcmd.NewInteractiveDeferredLoadingClientConfig(loadingRules, rootOpts.ConfigOverrides, os.Stdin)

	command.PersistentFlags().StringVar(&rootOpts.cfgFile, "config", "", "config file (default is $HOME/config.yaml)")
	command.PersistentFlags().BoolVarP(&rootOpts.showSource, "show-source", "s", false, "Show line number for errors")

	rootOpts.configAccessor = viper.NewAccessor(config.Options{StrictMode: true})
	// Here you will define your flags and configuration settings. Cobra supports persistent flags, which, if defined
	// here, will be global for your application.
	rootOpts.configAccessor.InitializePflags(command.PersistentFlags())

	command.AddCommand(viper.GetConfigCommand())

	return command
}

func (r *RootOptions) initConfig(_ *cobra.Command, _ []string) error {
	r.configAccessor = viper.NewAccessor(config.Options{
		StrictMode:  true,
		SearchPaths: []string{r.cfgFile},
	})

	err := r.configAccessor.UpdateConfig(context.TODO())
	if err != nil {
		return err
	}

	return nil
}

func (r *RootOptions) ConfigureClient() error {
	restConfig, err := r.clientConfig.ClientConfig()
	if err != nil {
		return err
	}
	k, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return err
	}
	r.restConfig = restConfig
	r.kubeClient = k
	return nil
}

func init() {
	klog.InitFlags(flag.CommandLine)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		logger.Error(context.TODO(), "Error in initializing: %v", err)
		os.Exit(-1)
	}
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey, contextutils.TaskIDKey)
}
