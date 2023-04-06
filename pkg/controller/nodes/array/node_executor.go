package array

import (
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
)

type arrayNodeExecutor struct {
	interfaces.Node
}

/*
TODO @hamersaw - override NewNodeExecutionContext function
*/

func newArrayNodeExecutor(nodeExecutor interfaces.Node) arrayNodeExecutor {
	return arrayNodeExecutor{
		Node: nodeExecutor,
	}
}
