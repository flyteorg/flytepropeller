package crd

import (
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	"log"

	"github.com/kubeflow/crd-validation/pkg/crd/exporter"
	"github.com/kubeflow/crd-validation/pkg/utils"
)

const (
	// CRDName is the name for FlyteWorkflow.
	CRDNameFlyteWorkflow = "github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1.FlyteWorkflow"

	generatedFileFlyteWorkflow = "flyteworkflow-crd-v1alpha1.yaml"
)

// FlyteWorkflowGenerator is the type for FlyteWorkflow CRD generator.
type FlyteWorkflowGenerator struct {
	*exporter.Exporter
}

// Creates a new CRD generator which outputs to a file.
func NewFlyteWorkflowGenerator(outputDir string) *FlyteWorkflowGenerator {
	return &FlyteWorkflowGenerator{
		Exporter: exporter.NewFileExporter(outputDir, generatedFileFlyteWorkflow),
	}
}

// Creates a new CRD generator which outputs to stdout.
func NewFlyteWorkflowGeneratorStdout() *FlyteWorkflowGenerator {
	return &FlyteWorkflowGenerator{
		Exporter: exporter.NewStdoutExporter(),
	}
}

// Generate generates the crd.
func (t FlyteWorkflowGenerator) Generate(original *apiextensions.CustomResourceDefinition) *apiextensions.CustomResourceDefinition {
	log.Println("Generating validation")
	original.Spec.Validation = utils.GetCustomResourceValidation(CRDNameFlyteWorkflow, v1alpha1.GetOpenAPIDefinitions)
	return original
}