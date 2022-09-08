package subworkflow

import (
	"strconv"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/encoding"
)

const maxLengthForSubWorkflow = 20

func GetChildWorkflowExecutionID(nodeExecID *core.NodeExecutionIdentifier, attempt uint32) (*core.WorkflowExecutionIdentifier, error) {
	name, err := encoding.FixedLengthUniqueIDForParts(maxLengthForSubWorkflow, nodeExecID.ExecutionId.Name, nodeExecID.NodeId, strconv.Itoa(int(attempt)))
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

func GetChildWorkflowExecutionIDV2(nodeExecID *core.NodeExecutionIdentifier, attempt uint32) (*core.WorkflowExecutionIdentifier, error) {
	name, err := encoding.FixedLengthUniqueIDForParts(maxLengthForSubWorkflow, nodeExecID.ExecutionId.Name, nodeExecID.NodeId, strconv.Itoa(int(attempt)))
	if err != nil {
		return nil, err
	}

	// Restriction on name is 20 chars
	return &core.WorkflowExecutionIdentifier{
		Project: nodeExecID.ExecutionId.Project,
		Domain:  nodeExecID.ExecutionId.Domain,
		Name:    EnsureExecIDWithinLength(nodeExecID.ExecutionId.Name, name, maxLengthForSubWorkflow),
	}, nil
}

func EnsureExecIDWithinLength(execID, subName string, maxLength int) string {
	maxLengthRemaining := maxLength - len(subName)
	if len(execID) < maxLengthRemaining {
		return execID + subName
	}

	return execID[:maxLengthRemaining] + subName
}
