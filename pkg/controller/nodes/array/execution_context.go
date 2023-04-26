package array

import (
	"context"
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
	if executionConfig.EnvironmentVariables == nil {
		executionConfig.EnvironmentVariables = make(map[string]string)
	}
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
	nodeStatus       *v1alpha1.NodeStatus
}

func (a arrayNodeExecutionContext) InputReader() io.InputReader {
	return a.inputReader
}

func (a arrayNodeExecutionContext) ExecutionContext() executors.ExecutionContext {
	return a.executionContext
}

func (a arrayNodeExecutionContext) NodeStatus() v1alpha1.ExecutableNodeStatus {
	return a.nodeStatus
}

func newArrayNodeExecutionContext(nodeExecutionContext interfaces.NodeExecutionContext, inputReader io.InputReader, subNodeIndex int, nodeStatus *v1alpha1.NodeStatus) arrayNodeExecutionContext {
	arrayExecutionContext := newArrayExecutionContext(nodeExecutionContext.ExecutionContext(), subNodeIndex)
	return arrayNodeExecutionContext{
		NodeExecutionContext: nodeExecutionContext,
		inputReader:          inputReader,
		executionContext:     arrayExecutionContext,
		nodeStatus:           nodeStatus,
	}
}


type arrayNodeExecutionContextBuilder struct {
	nCtxBuilder   interfaces.NodeExecutionContextBuilder
	subNodeID     v1alpha1.NodeID
	subNodeIndex  int
	subNodeStatus *v1alpha1.NodeStatus
	inputReader   io.InputReader
}

func (a *arrayNodeExecutionContextBuilder) BuildNodeExecutionContext(ctx context.Context, executionContext executors.ExecutionContext,
	nl executors.NodeLookup, currentNodeID v1alpha1.NodeID) (interfaces.NodeExecutionContext, error) {

	// create base NodeExecutionContext
	nCtx, err := a.nCtxBuilder.BuildNodeExecutionContext(ctx, executionContext, nl, currentNodeID)
	if err != nil {
		return nil, err
	}

	if currentNodeID == a.subNodeID {
		// overwrite NodeExecutionContext for ArrayNode execution
		nCtx = newArrayNodeExecutionContext(nCtx, a.inputReader, a.subNodeIndex, a.subNodeStatus)
	}

	return nCtx, nil
}

func newArrayNodeExecutionContextBuilder(nCtxBuilder interfaces.NodeExecutionContextBuilder, subNodeID v1alpha1.NodeID,
	subNodeIndex int, subNodeStatus *v1alpha1.NodeStatus, inputReader io.InputReader) interfaces.NodeExecutionContextBuilder {

	return &arrayNodeExecutionContextBuilder{
		nCtxBuilder:   nCtxBuilder,
		subNodeID:     subNodeID,
		subNodeIndex:  subNodeIndex,
		subNodeStatus: subNodeStatus,
		inputReader:   inputReader,
	}
}
