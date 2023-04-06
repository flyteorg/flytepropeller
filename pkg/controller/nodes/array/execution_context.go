package array

import (
	//"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
)

type arrayNodeExecutionContext struct {
	handler.NodeExecutionContext
}

// TODO @hamersaw - overwrite everything
/*
inputReader
taskRecorder
nodeRecorder - need to add to nodeExecutionContext so we can override?!?!
maxParallelism - looks like we need:
	ExecutionConfig.GetMaxParallelism
	ExecutionContext.IncrementMaxParallelism
storage locations
	dataPrefix

add environment variables for maptask execution either:
	(1) in arrayExecutionContext if we use separate for each
	(2) in arrayNodeExectionContext if we choose to use single DAG
*/

/*func newArrayExecutionContext(executionContext executors.ExecutionContext) executors.ExecutionContext {
	return arrayExecutionContext{
		ExecutionContext: executionContext,
	}
}*/

func newArrayNodeExecutionContext(nodeExecutionContext handler.NodeExecutionContext) arrayNodeExecutionContext {
	return arrayNodeExecutionContext{
		NodeExecutionContext: nodeExecutionContext,
	}
}
