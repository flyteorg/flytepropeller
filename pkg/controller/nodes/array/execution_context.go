package array

import (
	"strconv"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
)

const (
	FlyteK8sArrayIndexVarName string = "FLYTE_K8S_ARRAY_INDEX"
	JobIndexVarName           string = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
)

type arrayExecutionContext struct {
	executors.ExecutionContext
	executionConfig v1alpha1.ExecutionConfig
}

func (a arrayExecutionContext) GetExecutionConfig() v1alpha1.ExecutionConfig {
	return a.executionConfig
}

func newArrayExecutionContext(executionContext executors.ExecutionContext, subNodeIndex int) arrayExecutionContext {
	executionConfig := executionContext.GetExecutionConfig()
	executionConfig.EnvironmentVariables[JobIndexVarName] = FlyteK8sArrayIndexVarName
	executionConfig.EnvironmentVariables[FlyteK8sArrayIndexVarName] = strconv.Itoa(subNodeIndex)

	return arrayExecutionContext{
		ExecutionContext: executionContext,
		executionConfig:  executionConfig,
	}
}

type arrayNodeExecutionContext struct {
	interfaces.NodeExecutionContext
	inputReader      io.InputReader
	executionContext arrayExecutionContext
}

func (a arrayNodeExecutionContext) InputReader() io.InputReader {
	return a.inputReader
}

func (a arrayNodeExecutionContext) ExecutionContext() executors.ExecutionContext {
	return a.executionContext
}

// TODO @hamersaw - overwrite everything
/*
inputReader
taskRecorder
nodeRecorder - need to add to nodeExecutionContext so we can override?!?!
maxParallelism - looks like we need:
	ExecutionConfig.GetMaxParallelism
	ExecutionContext.IncrementMaxParallelism
storage locations - dataPrefix?

add environment variables for maptask execution either:
	(1) in arrayExecutionContext if we use separate for each
	(2) in arrayNodeExectionContext if we choose to use single DAG
*/

func newArrayNodeExecutionContext(nodeExecutionContext interfaces.NodeExecutionContext, inputReader io.InputReader, subNodeIndex int) arrayNodeExecutionContext {
	arrayExecutionContext := newArrayExecutionContext(nodeExecutionContext.ExecutionContext(), subNodeIndex)
	return arrayNodeExecutionContext{
		NodeExecutionContext: nodeExecutionContext,
		inputReader:          inputReader,
		executionContext:     arrayExecutionContext,
	}
}
