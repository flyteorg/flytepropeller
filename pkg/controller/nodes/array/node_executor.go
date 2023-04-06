package array

import (
	"context"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
)

type arrayNodeExecutor struct {
	interfaces.Node
	subNodeID   v1alpha1.NodeID
	inputReader io.InputReader
}

// TODO @hamersaw - docs
func (a *arrayNodeExecutor) NewNodeExecutionContext(ctx context.Context, executionContext executors.ExecutionContext,
	nl executors.NodeLookup, currentNodeID v1alpha1.NodeID) (interfaces.NodeExecutionContext, error) {

	// create base NodeExecutionContext
	nCtx, err := a.Node.NewNodeExecutionContext(ctx, executionContext, nl, currentNodeID)
	if err != nil {
		return nil, err
	}

	if currentNodeID == a.subNodeID {
		// TODO @hamersaw - overwrite NodeExecutionContext for ArrayNode execution
	}

	return nCtx, nil
}

func newArrayNodeExecutor(nodeExecutor interfaces.Node, subNodeID v1alpha1.NodeID, inputReader io.InputReader) arrayNodeExecutor {
	return arrayNodeExecutor{
		Node:        nodeExecutor,
		subNodeID:   subNodeID,
		inputReader: inputReader,
	}
}
