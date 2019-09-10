package task

import (
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	structpb "github.com/golang/protobuf/ptypes/struct"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/stretchr/testify/assert"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

func TestToTaskEventPhase(t *testing.T) {
	assert.Equal(t, core.TaskExecution_UNDEFINED, ToTaskEventPhase(pluginCore.PhaseUndefined))
	assert.Equal(t, core.TaskExecution_SUCCEEDED, ToTaskEventPhase(pluginCore.PhaseSuccess))
	assert.Equal(t, core.TaskExecution_RUNNING, ToTaskEventPhase(pluginCore.PhaseRunning))
	assert.Equal(t, core.TaskExecution_FAILED, ToTaskEventPhase(pluginCore.PhasePermanentFailure))
	assert.Equal(t, core.TaskExecution_FAILED, ToTaskEventPhase(pluginCore.PhaseRetryableFailure))
	assert.Equal(t, core.TaskExecution_QUEUED, ToTaskEventPhase(pluginCore.PhaseWaitingForResources))
	assert.Equal(t, core.TaskExecution_QUEUED, ToTaskEventPhase(pluginCore.PhaseInitializing))
	assert.Equal(t, core.TaskExecution_UNDEFINED, ToTaskEventPhase(pluginCore.PhaseNotReady))
	assert.Equal(t, core.TaskExecution_QUEUED, ToTaskEventPhase(pluginCore.PhaseQueued))
}

func TestToTaskExecutionEvent(t *testing.T) {
	id := &core.Identifier{}
	n := time.Now()
	np, _ := ptypes.TimestampProto(n)

	tev, err := ToTaskExecutionEvent(id, pluginCore.PhaseInfoWaitingForResources(n, 0, "reason"))
	assert.NoError(t, err)
	assert.Nil(t, tev.Logs)
	assert.Equal(t, core.TaskExecution_QUEUED, tev.Phase)
	assert.Equal(t, uint32(0), tev.PhaseVersion)
	assert.Equal(t, np, tev.OccurredAt)

	l := []*core.TaskLog{
		{Uri: "x", Name: "y", MessageFormat: core.TaskLog_JSON},
	}
	c := &structpb.Struct{}
	tev, err = ToTaskExecutionEvent(id, pluginCore.PhaseInfoRunning(1, &pluginCore.TaskInfo{
		OccurredAt: &n,
		Logs:       l,
		CustomInfo: c,
	}))
	assert.NoError(t, err)
	assert.Equal(t, core.TaskExecution_RUNNING, tev.Phase)
	assert.Equal(t, uint32(1), tev.PhaseVersion)
	assert.Equal(t, l, tev.Logs)
	assert.Equal(t, c, tev.CustomInfo)
	assert.Equal(t, np, tev.OccurredAt)
}

func TestToTransitionType(t *testing.T) {
	assert.Equal(t, handler.TransitionTypeEphemeral, ToTransitionType(pluginCore.TransitionTypeEphemeral))
	assert.Equal(t, handler.TransitionTypeEphemeral, ToTransitionType(pluginCore.TransitionTypeBestEffort))
	assert.Equal(t, handler.TransitionTypeBarrier, ToTransitionType(pluginCore.TransitionTypeBarrier))
}
