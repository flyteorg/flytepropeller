package task

import (
	"testing"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	mocks2 "github.com/flyteorg/flytepropeller/pkg/controller/executors/mocks"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/event"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io/mocks"
	"github.com/flyteorg/flytestdlib/storage"
	"github.com/golang/protobuf/ptypes"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/assert"

	pluginMocks "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	handlerMocks "github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler/mocks"
)

const containerTaskType = "container"
const containerPluginIdentifier = "container_plugin"

func TestToTaskEventPhase(t *testing.T) {
	assert.Equal(t, core.TaskExecution_UNDEFINED, ToTaskEventPhase(pluginCore.PhaseUndefined))
	assert.Equal(t, core.TaskExecution_SUCCEEDED, ToTaskEventPhase(pluginCore.PhaseSuccess))
	assert.Equal(t, core.TaskExecution_RUNNING, ToTaskEventPhase(pluginCore.PhaseRunning))
	assert.Equal(t, core.TaskExecution_FAILED, ToTaskEventPhase(pluginCore.PhasePermanentFailure))
	assert.Equal(t, core.TaskExecution_FAILED, ToTaskEventPhase(pluginCore.PhaseRetryableFailure))
	assert.Equal(t, core.TaskExecution_WAITING_FOR_RESOURCES, ToTaskEventPhase(pluginCore.PhaseWaitingForResources))
	assert.Equal(t, core.TaskExecution_INITIALIZING, ToTaskEventPhase(pluginCore.PhaseInitializing))
	assert.Equal(t, core.TaskExecution_UNDEFINED, ToTaskEventPhase(pluginCore.PhaseNotReady))
	assert.Equal(t, core.TaskExecution_QUEUED, ToTaskEventPhase(pluginCore.PhaseQueued))
}

func Test_trimErrorMessage(t *testing.T) {
	const inputStr = "0123456789"
	t.Run("Length less or equal than max", func(t *testing.T) {
		input := inputStr
		assert.Equal(t, input, trimErrorMessage(input, 10))
	})

	t.Run("Length > max", func(t *testing.T) {
		input := inputStr
		assert.Equal(t, "01236789", trimErrorMessage(input, 8))
	})

	t.Run("Odd Max", func(t *testing.T) {
		input := inputStr
		assert.Equal(t, "01236789", trimErrorMessage(input, 9))
	})

	t.Run("Odd input", func(t *testing.T) {
		input := "012345678"
		assert.Equal(t, "012345678", trimErrorMessage(input, 9))
	})
}

func TestToTaskExecutionEvent(t *testing.T) {
	tkID := &core.Identifier{}
	nodeID := &core.NodeExecutionIdentifier{}
	id := &core.TaskExecutionIdentifier{
		TaskId:          tkID,
		NodeExecutionId: nodeID,
	}
	n := time.Now()
	np, _ := ptypes.TimestampProto(n)

	in := &mocks.InputFilePaths{}
	const inputPath = "in"
	in.On("GetInputPath").Return(storage.DataReference(inputPath))

	out := &mocks.OutputFilePaths{}
	const outputPath = "out"
	out.On("GetOutputPath").Return(storage.DataReference(outputPath))

	nodeExecutionMetadata := handlerMocks.NodeExecutionMetadata{}
	nodeExecutionMetadata.OnIsInterruptible().Return(true)

	mockExecContext := &mocks2.ExecutionContext{}
	mockExecContext.OnGetEventVersion().Return(v1alpha1.EventVersion0)
	mockExecContext.OnGetParentInfo().Return(nil)

	tID := &pluginMocks.TaskExecutionID{}
	generatedName := "generated_name"
	tID.OnGetGeneratedName().Return(generatedName)
	tID.OnGetID().Return(*id)

	tMeta := &pluginMocks.TaskExecutionMetadata{}
	tMeta.OnGetTaskExecutionID().Return(tID)

	tCtx := &pluginMocks.TaskExecutionContext{}
	tCtx.OnTaskExecutionMetadata().Return(tMeta)
	resourcePoolInfo := []*event.ResourcePoolInfo{
		{
			Namespace:       "ns",
			AllocationToken: "alloc_token",
		},
	}

	tev, err := ToTaskExecutionEvent(ToTaskExecutionEventInputs{
		TaskExecContext: tCtx,
		InputReader:     in,
		OutputWriter:    out,
		Info: pluginCore.PhaseInfoWaitingForResourcesInfo(n, 0, "reason", &pluginCore.TaskInfo{
			OccurredAt: &n,
		}),
		NodeExecutionMetadata: &nodeExecutionMetadata,
		ExecContext:           mockExecContext,
		TaskType:              containerTaskType,
		PluginID:              containerPluginIdentifier,
		ResourcePoolInfo:      resourcePoolInfo,
		ClusterID:             testClusterID,
	})
	assert.NoError(t, err)
	assert.Nil(t, tev.Logs)
	assert.Equal(t, core.TaskExecution_WAITING_FOR_RESOURCES, tev.Phase)
	assert.Equal(t, uint32(0), tev.PhaseVersion)
	assert.Equal(t, np, tev.OccurredAt)
	assert.Equal(t, tkID, tev.TaskId)
	assert.Equal(t, nodeID, tev.ParentNodeExecutionId)
	assert.Equal(t, inputPath, tev.InputUri)
	assert.Nil(t, tev.OutputResult)
	assert.Equal(t, event.TaskExecutionMetadata_INTERRUPTIBLE, tev.Metadata.InstanceClass)
	assert.Equal(t, containerTaskType, tev.TaskType)
	assert.Equal(t, "reason", tev.Reason)
	assert.Equal(t, containerPluginIdentifier, tev.Metadata.PluginIdentifier)
	assert.Equal(t, generatedName, tev.Metadata.GeneratedName)
	assert.EqualValues(t, resourcePoolInfo, tev.Metadata.ResourcePoolInfo)
	assert.Equal(t, testClusterID, tev.ProducerId)

	l := []*core.TaskLog{
		{Uri: "x", Name: "y", MessageFormat: core.TaskLog_JSON},
	}
	c := &structpb.Struct{}
	tev, err = ToTaskExecutionEvent(ToTaskExecutionEventInputs{
		TaskExecContext: tCtx,
		InputReader:     in,
		OutputWriter:    out,
		Info: pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{
			OccurredAt: &n,
			Logs:       l,
			CustomInfo: c,
		}),
		NodeExecutionMetadata: &nodeExecutionMetadata,
		ExecContext:           mockExecContext,
		TaskType:              containerTaskType,
		PluginID:              containerPluginIdentifier,
		ResourcePoolInfo:      resourcePoolInfo,
		ClusterID:             testClusterID,
	})
	assert.NoError(t, err)
	assert.Equal(t, core.TaskExecution_RUNNING, tev.Phase)
	assert.Equal(t, uint32(1), tev.PhaseVersion)
	assert.Equal(t, l, tev.Logs)
	assert.Equal(t, c, tev.CustomInfo)
	assert.Equal(t, np, tev.OccurredAt)
	assert.Equal(t, tkID, tev.TaskId)
	assert.Equal(t, nodeID, tev.ParentNodeExecutionId)
	assert.Equal(t, inputPath, tev.InputUri)
	assert.Nil(t, tev.OutputResult)
	assert.Equal(t, event.TaskExecutionMetadata_INTERRUPTIBLE, tev.Metadata.InstanceClass)
	assert.Equal(t, containerTaskType, tev.TaskType)
	assert.Equal(t, containerPluginIdentifier, tev.Metadata.PluginIdentifier)
	assert.Equal(t, generatedName, tev.Metadata.GeneratedName)
	assert.EqualValues(t, resourcePoolInfo, tev.Metadata.ResourcePoolInfo)
	assert.Equal(t, testClusterID, tev.ProducerId)

	defaultNodeExecutionMetadata := handlerMocks.NodeExecutionMetadata{}
	defaultNodeExecutionMetadata.OnIsInterruptible().Return(false)
	tev, err = ToTaskExecutionEvent(ToTaskExecutionEventInputs{
		TaskExecContext: tCtx,
		InputReader:     in,
		OutputWriter:    out,
		Info: pluginCore.PhaseInfoSuccess(&pluginCore.TaskInfo{
			OccurredAt: &n,
			Logs:       l,
			CustomInfo: c,
		}),
		NodeExecutionMetadata: &defaultNodeExecutionMetadata,
		ExecContext:           mockExecContext,
		TaskType:              containerTaskType,
		PluginID:              containerPluginIdentifier,
		ResourcePoolInfo:      resourcePoolInfo,
		ClusterID:             testClusterID,
	})
	assert.NoError(t, err)
	assert.Equal(t, core.TaskExecution_SUCCEEDED, tev.Phase)
	assert.Equal(t, uint32(0), tev.PhaseVersion)
	assert.Equal(t, l, tev.Logs)
	assert.Equal(t, c, tev.CustomInfo)
	assert.Equal(t, np, tev.OccurredAt)
	assert.Equal(t, np, tev.OccurredAt)
	assert.Equal(t, tkID, tev.TaskId)
	assert.Equal(t, nodeID, tev.ParentNodeExecutionId)
	assert.NotNil(t, tev.OutputResult)
	assert.Equal(t, inputPath, tev.InputUri)
	assert.Equal(t, outputPath, tev.GetOutputUri())
	assert.Empty(t, event.TaskExecutionMetadata_DEFAULT, tev.Metadata.InstanceClass)
	assert.Equal(t, containerTaskType, tev.TaskType)
	assert.Equal(t, containerPluginIdentifier, tev.Metadata.PluginIdentifier)
	assert.Equal(t, generatedName, tev.Metadata.GeneratedName)
	assert.EqualValues(t, resourcePoolInfo, tev.Metadata.ResourcePoolInfo)
	assert.Equal(t, testClusterID, tev.ProducerId)
}

func TestToTransitionType(t *testing.T) {
	assert.Equal(t, handler.TransitionTypeEphemeral, ToTransitionType(pluginCore.TransitionTypeEphemeral))
	assert.Equal(t, handler.TransitionTypeBarrier, ToTransitionType(pluginCore.TransitionTypeBarrier))
}

func TestToTaskExecutionEventWithParent(t *testing.T) {
	tkID := &core.Identifier{}

	nodeID := &core.NodeExecutionIdentifier{
		NodeId: "n1234567812345678123344568",
	}
	id := &core.TaskExecutionIdentifier{
		TaskId:          tkID,
		NodeExecutionId: nodeID,
	}
	n := time.Now()
	np, _ := ptypes.TimestampProto(n)

	in := &mocks.InputFilePaths{}
	const inputPath = "in"
	in.On("GetInputPath").Return(storage.DataReference(inputPath))

	out := &mocks.OutputFilePaths{}
	const outputPath = "out"
	out.On("GetOutputPath").Return(storage.DataReference(outputPath))

	nodeExecutionMetadata := handlerMocks.NodeExecutionMetadata{}
	nodeExecutionMetadata.OnIsInterruptible().Return(true)

	mockExecContext := &mocks2.ExecutionContext{}
	mockExecContext.OnGetEventVersion().Return(v1alpha1.EventVersion1)
	mockParentInfo := &mocks2.ImmutableParentInfo{}
	mockParentInfo.OnGetUniqueID().Return("np1")
	mockParentInfo.OnCurrentAttempt().Return(uint32(2))
	mockExecContext.OnGetParentInfo().Return(mockParentInfo)

	tID := &pluginMocks.TaskExecutionID{}
	generatedName := "generated_name"
	tID.OnGetGeneratedName().Return(generatedName)
	tID.OnGetID().Return(*id)

	tMeta := &pluginMocks.TaskExecutionMetadata{}
	tMeta.OnGetTaskExecutionID().Return(tID)

	tCtx := &pluginMocks.TaskExecutionContext{}
	tCtx.OnTaskExecutionMetadata().Return(tMeta)
	resourcePoolInfo := []*event.ResourcePoolInfo{
		{
			Namespace:       "ns",
			AllocationToken: "alloc_token",
		},
	}

	tev, err := ToTaskExecutionEvent(ToTaskExecutionEventInputs{
		TaskExecContext: tCtx,
		InputReader:     in,
		OutputWriter:    out,
		Info: pluginCore.PhaseInfoWaitingForResourcesInfo(n, 0, "reason", &pluginCore.TaskInfo{
			OccurredAt: &n,
		}),
		NodeExecutionMetadata: &nodeExecutionMetadata,
		ExecContext:           mockExecContext,
		TaskType:              containerTaskType,
		PluginID:              containerPluginIdentifier,
		ResourcePoolInfo:      resourcePoolInfo,
		ClusterID:             testClusterID,
	})
	assert.NoError(t, err)
	expectedNodeID := &core.NodeExecutionIdentifier{
		NodeId: "fmxzd5ta",
	}
	assert.Nil(t, tev.Logs)
	assert.Equal(t, core.TaskExecution_WAITING_FOR_RESOURCES, tev.Phase)
	assert.Equal(t, uint32(0), tev.PhaseVersion)
	assert.Equal(t, np, tev.OccurredAt)
	assert.Equal(t, tkID, tev.TaskId)
	assert.Equal(t, expectedNodeID, tev.ParentNodeExecutionId)
	assert.Equal(t, inputPath, tev.InputUri)
	assert.Nil(t, tev.OutputResult)
	assert.Equal(t, event.TaskExecutionMetadata_INTERRUPTIBLE, tev.Metadata.InstanceClass)
	assert.Equal(t, containerTaskType, tev.TaskType)
	assert.Equal(t, "reason", tev.Reason)
	assert.Equal(t, containerPluginIdentifier, tev.Metadata.PluginIdentifier)
	assert.Equal(t, generatedName, tev.Metadata.GeneratedName)
	assert.EqualValues(t, resourcePoolInfo, tev.Metadata.ResourcePoolInfo)
	assert.Equal(t, testClusterID, tev.ProducerId)

	l := []*core.TaskLog{
		{Uri: "x", Name: "y", MessageFormat: core.TaskLog_JSON},
	}
	c := &structpb.Struct{}
	tev, err = ToTaskExecutionEvent(ToTaskExecutionEventInputs{
		TaskExecContext: tCtx,
		InputReader:     in,
		OutputWriter:    out,
		Info: pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{
			OccurredAt: &n,
			Logs:       l,
			CustomInfo: c,
		}),
		NodeExecutionMetadata: &nodeExecutionMetadata,
		ExecContext:           mockExecContext,
		TaskType:              containerTaskType,
		PluginID:              containerPluginIdentifier,
		ResourcePoolInfo:      resourcePoolInfo,
		ClusterID:             testClusterID,
	})
	assert.NoError(t, err)
	assert.Equal(t, core.TaskExecution_RUNNING, tev.Phase)
	assert.Equal(t, uint32(1), tev.PhaseVersion)
	assert.Equal(t, l, tev.Logs)
	assert.Equal(t, c, tev.CustomInfo)
	assert.Equal(t, np, tev.OccurredAt)
	assert.Equal(t, tkID, tev.TaskId)
	assert.Equal(t, expectedNodeID, tev.ParentNodeExecutionId)
	assert.Equal(t, inputPath, tev.InputUri)
	assert.Nil(t, tev.OutputResult)
	assert.Equal(t, event.TaskExecutionMetadata_INTERRUPTIBLE, tev.Metadata.InstanceClass)
	assert.Equal(t, containerTaskType, tev.TaskType)
	assert.Equal(t, containerPluginIdentifier, tev.Metadata.PluginIdentifier)
	assert.Equal(t, generatedName, tev.Metadata.GeneratedName)
	assert.EqualValues(t, resourcePoolInfo, tev.Metadata.ResourcePoolInfo)
	assert.Equal(t, testClusterID, tev.ProducerId)
}
