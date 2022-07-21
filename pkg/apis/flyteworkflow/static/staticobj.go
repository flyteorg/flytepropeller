package static

import (
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type WorkflowStaticExecutionObj struct {
	*v1alpha1.WorkflowSpec `json:"spec"`
	SubWorkflows           map[v1alpha1.WorkflowID]*v1alpha1.WorkflowSpec `json:"subWorkflows,omitempty"`
	Tasks                  map[v1alpha1.TaskID]*v1alpha1.TaskSpec         `json:"tasks"`
}
