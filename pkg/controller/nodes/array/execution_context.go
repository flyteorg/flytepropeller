package array

import (
	"context"
	"strconv"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
)

const (
	FlyteK8sArrayIndexVarName string = "FLYTE_K8S_ARRAY_INDEX"
	JobIndexVarName           string = "BATCH_JOB_ARRAY_INDEX_VAR_NAME"
)

type arrayExecutionContext struct {
	executors.ExecutionContext
	executionConfig    v1alpha1.ExecutionConfig
	currentParallelism *uint32
}

func (a *arrayExecutionContext) GetExecutionConfig() v1alpha1.ExecutionConfig {
	return a.executionConfig
}

func (a *arrayExecutionContext) CurrentParallelism() uint32 {
	return *a.currentParallelism
}

func (a *arrayExecutionContext) IncrementParallelism() uint32 {
	*a.currentParallelism = *a.currentParallelism+1
	return *a.currentParallelism
}

func newArrayExecutionContext(executionContext executors.ExecutionContext, subNodeIndex int, currentParallelism *uint32, maxParallelism uint32) *arrayExecutionContext {
	executionConfig := executionContext.GetExecutionConfig()
	if executionConfig.EnvironmentVariables == nil {
		executionConfig.EnvironmentVariables = make(map[string]string)
	}
	executionConfig.EnvironmentVariables[JobIndexVarName] = FlyteK8sArrayIndexVarName
	executionConfig.EnvironmentVariables[FlyteK8sArrayIndexVarName] = strconv.Itoa(subNodeIndex)
	
	executionConfig.MaxParallelism = maxParallelism

	return &arrayExecutionContext{
		ExecutionContext:   executionContext,
		executionConfig:    executionConfig,
		currentParallelism: currentParallelism,
	}
}

type arrayEventRecorder struct {}

func (a *arrayEventRecorder) RecordNodeEvent(ctx context.Context, event *event.NodeExecutionEvent, eventConfig *config.EventConfig) error {
	return nil
}

func (a *arrayEventRecorder) RecordTaskEvent(ctx context.Context, event *event.TaskExecutionEvent, eventConfig *config.EventConfig) error {
	return nil
}

type arrayTaskReader struct {
	interfaces.TaskReader
}

func (a *arrayTaskReader) Read(ctx context.Context) (*core.TaskTemplate, error) {
	taskTemplate, err := a.TaskReader.Read(ctx)
	if err != nil {
		return nil, err
	}

	// convert output list variable to singular
	outputVariables := make(map[string]*core.Variable)
	for key, value := range taskTemplate.Interface.Outputs.Variables {
		switch v := value.Type.Type.(type) {
		case *core.LiteralType_CollectionType:
			outputVariables[key] = &core.Variable{
				Type:        v.CollectionType,
				Description: value.Description,
			}
		default:
			outputVariables[key] = value
		}
	}

	taskTemplate.Interface.Outputs = &core.VariableMap{
		Variables: outputVariables,
	}
	return taskTemplate, nil
}


type arrayNodeExecutionContext struct {
	interfaces.NodeExecutionContext
	eventRecorder    *arrayEventRecorder
	executionContext *arrayExecutionContext
	inputReader      io.InputReader
	nodeStatus       *v1alpha1.NodeStatus
	taskReader       interfaces.TaskReader
}

func (a *arrayNodeExecutionContext) EventsRecorder() interfaces.EventRecorder {
	return a.eventRecorder
}

func (a *arrayNodeExecutionContext) ExecutionContext() executors.ExecutionContext {
	return a.executionContext
}

func (a *arrayNodeExecutionContext) InputReader() io.InputReader {
	return a.inputReader
}

func (a *arrayNodeExecutionContext) NodeStatus() v1alpha1.ExecutableNodeStatus {
	return a.nodeStatus
}

func (a *arrayNodeExecutionContext) TaskReader() interfaces.TaskReader {
	return a.taskReader
}

func newArrayNodeExecutionContext(nodeExecutionContext interfaces.NodeExecutionContext, inputReader io.InputReader, subNodeIndex int, nodeStatus *v1alpha1.NodeStatus, currentParallelism *uint32, maxParallelism uint32) *arrayNodeExecutionContext {
	arrayExecutionContext := newArrayExecutionContext(nodeExecutionContext.ExecutionContext(), subNodeIndex, currentParallelism, maxParallelism)
	return &arrayNodeExecutionContext{
		NodeExecutionContext: nodeExecutionContext,
		eventRecorder:        &arrayEventRecorder{},
		executionContext:     arrayExecutionContext,
		inputReader:          inputReader,
		nodeStatus:           nodeStatus,
		taskReader:           &arrayTaskReader{nodeExecutionContext.TaskReader()},
	}
}

type arrayNodeExecutionContextBuilder struct {
	nCtxBuilder        interfaces.NodeExecutionContextBuilder
	subNodeID          v1alpha1.NodeID
	subNodeIndex       int
	subNodeStatus      *v1alpha1.NodeStatus
	inputReader        io.InputReader
	currentParallelism *uint32
	maxParallelism     uint32
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
		nCtx = newArrayNodeExecutionContext(nCtx, a.inputReader, a.subNodeIndex, a.subNodeStatus, a.currentParallelism, a.maxParallelism)
	}

	return nCtx, nil
}

func newArrayNodeExecutionContextBuilder(nCtxBuilder interfaces.NodeExecutionContextBuilder, subNodeID v1alpha1.NodeID,
	subNodeIndex int, subNodeStatus *v1alpha1.NodeStatus, inputReader io.InputReader, currentParallelism *uint32, maxParallelism uint32) interfaces.NodeExecutionContextBuilder {

	return &arrayNodeExecutionContextBuilder{
		nCtxBuilder:        nCtxBuilder,
		subNodeID:          subNodeID,
		subNodeIndex:       subNodeIndex,
		subNodeStatus:      subNodeStatus,
		inputReader:        inputReader,
		currentParallelism: currentParallelism,
		maxParallelism:     maxParallelism,
	}
}
