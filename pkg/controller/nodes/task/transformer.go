package task

import (
	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

func ToTransitionType(ttype pluginCore.TransitionType) handler.TransitionType {
	if ttype == pluginCore.TransitionTypeBarrier {
		return handler.TransitionTypeBarrier
	}
	return handler.TransitionTypeEphemeral
}

func ToTaskEventPhase(p pluginCore.Phase) core.TaskExecution_Phase {
	switch p {
	case pluginCore.PhaseQueued:
		return core.TaskExecution_QUEUED
	case pluginCore.PhaseInitializing:
		// TODO add initializing phase
		return core.TaskExecution_QUEUED
	case pluginCore.PhaseSuccess:
		return core.TaskExecution_SUCCEEDED
	case pluginCore.PhasePermanentFailure:
		return core.TaskExecution_FAILED
	case pluginCore.PhaseRetryableFailure:
		return core.TaskExecution_FAILED
	case pluginCore.PhaseNotReady:
		fallthrough
	case pluginCore.PhaseUndefined:
		fallthrough
	default:
		return core.TaskExecution_UNDEFINED
	}
}

func ToTaskExecutionEvent(tk *core.Identifier, info pluginCore.PhaseInfo) (*event.TaskExecutionEvent, error) {
	// Transitions to a new phase

	tm := ptypes.TimestampNow()
	var err error
	if info.Info().OccurredAt != nil {
		tm, err = ptypes.TimestampProto(*info.Info().OccurredAt)
		if err != nil {
			return nil, err
		}
	}

	return &event.TaskExecutionEvent{
		Phase:        ToTaskEventPhase(info.Phase()),
		PhaseVersion: info.Version(),
		OccurredAt:   tm,
		Logs:         info.Info().Logs,
		CustomInfo:   info.Info().CustomInfo,
		TaskId:       tk,
	}, nil
}
