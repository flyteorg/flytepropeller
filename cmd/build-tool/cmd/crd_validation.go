package cmd

import (
	"github.com/kubeflow/crd-validation/pkg/config"
	"github.com/spf13/cobra"

	"github.com/lyft/flytepropeller/cmd/build-tool/cmd/crd"
)

const (
	baseCrdKey = "base-crd"
)

const crdValidationCmdName = "crd-validation"

// The extra options needed by crd-validation command
type CrdValidationOpts struct {
	*RootOptions
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

			return crdValidationOpts.generateValidation()
		},
	}

	crdValidationCmd.Flags().StringVarP(&crdValidationOpts.baseCrdFile, baseCrdKey, "b", "", "Path to base CRD file.")
	crdValidationCmd.Flags().BoolVarP(&crdValidationOpts.dryRun, "dry-run", "d", false, "Compiles and transforms, but does not create a workflow. OutputsRef ts to STDOUT.")

	return crdValidationCmd
}

func (c *CrdValidationOpts) generateValidation() error {

	var generator *crd.FlyteWorkflowGenerator
	
	generator = crd.NewFlyteWorkflowGeneratorStdout()

	original := config.NewCustomResourceDefinition(c.baseCrdFile)
	final := generator.Generate(original)
	generator.Export(final)

	return nil
}
