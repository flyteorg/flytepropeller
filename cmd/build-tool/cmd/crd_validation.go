package cmd

import (
	"github.com/spf13/viper"
	"log"

	compilerErrors "github.com/lyft/flytepropeller/pkg/compiler/errors"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/kubeflow/crd-validation/pkg/config"
	"github.com/lyft/flytepropeller/cmd/build-tool/cmd/crd"
)

const (
	configKey  = "config-file"
	baseCrdKey = "base-crd"
)

const crdValidationCmdName = "crd-validation"

type CrdValidationOpts struct {
	*RootOptions
	configFile  string
	baseCrdFile string
	dryRun      bool
}

func NewCrdValidationCommand(opts *RootOptions) *cobra.Command {
	crdValidationOpts := &CrdValidationOpts{
		RootOptions: opts,
	}

	crdValidationCmd := &cobra.Command{
		Use:     crdValidationCmdName,
		Aliases: []string{"validate"},
		Short:   "Augment a CRD YAML file with validation section based on a base CRD file",
		Long:    ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requiredFlags(cmd, baseCrdKey); err != nil {
				return err
			}

			compilerErrors.SetIncludeSource()

			return crdValidationOpts.generateValidation()
		},
	}

	crdValidationCmd.Flags().StringVarP(&crdValidationOpts.configFile, configKey, "c", "", "Path of the config file for the execution of CRD validation")
	crdValidationCmd.Flags().StringVarP(&crdValidationOpts.baseCrdFile, baseCrdKey, "b", "", "Path to base CRD file.")
	crdValidationCmd.Flags().BoolVarP(&crdValidationOpts.dryRun, "dry-run", "d", false, "Compiles and transforms, but does not create a workflow. OutputsRef ts to STDOUT.")

	return crdValidationCmd
}

func (c *CrdValidationOpts) initConfig() error {
	if c.configFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(c.configFile)
		log.Println("Using config file:", viper.ConfigFileUsed())
	}

	viper.SetConfigType("yaml") // Set config type to yaml

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		return errors.Wrapf(err, "Failed to read config file.")
	} else {
		log.Println("Using config file:", viper.ConfigFileUsed())
	}
	return nil
}

func (c *CrdValidationOpts) generateValidation() error {

	err := c.initConfig()
	var generator *crd.FlyteWorkflowGenerator
	if err != nil {
		log.Println("Failed to initialize in when generating validation")
		return err
	} else {
		crdValidationConfig := config.GetCrdValidationConfig()
		generator = crd.NewFlyteWorkflowGenerator(crdValidationConfig.OutputDir)
	}

	original := config.NewCustomResourceDefinition(c.baseCrdFile)
	final := generator.Generate(original)
	generator.Export(final)

	return nil
}
