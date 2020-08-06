package subworkflow

import (
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"strconv"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
)

const maxLengthForSubWorkflow = 20

func GetChildWorkflowExecutionID(nodeExecID *core.NodeExecutionIdentifier, attempt uint32) (*core.WorkflowExecutionIdentifier, error) {
	name, err := v1alpha1.FixedLengthUniqueIDForParts(maxLengthForSubWorkflow, nodeExecID.ExecutionId.Name, nodeExecID.NodeId, strconv.Itoa(int(attempt)))
	if err != nil {
		return nil, err
	}
	// Restriction on name is 20 chars
	return &core.WorkflowExecutionIdentifier{
		Project: nodeExecID.ExecutionId.Project,
		Domain:  nodeExecID.ExecutionId.Domain,
		Name:    name,
	}, nil
}
