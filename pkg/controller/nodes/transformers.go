package nodes

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

func ToNodeExecOutput(info *handler.OutputInfo) *event.NodeExecutionEvent_OutputUri {
	if info == nil || info.OutputURI == "" {
		return nil
	}

	return &event.NodeExecutionEvent_OutputUri{
		OutputUri: info.OutputURI.String(),
	}
}

func ToNodeExecTargetMetadata(info *handler.WorkflowNodeInfo) *event.NodeExecutionEvent_WorkflowNodeMetadata {
	if info == nil || info.LaunchedWorkflowID == nil {
		return nil
	}
	return &event.NodeExecutionEvent_WorkflowNodeMetadata{
		WorkflowNodeMetadata: &event.WorkflowNodeMetadata{
			ExecutionId: info.LaunchedWorkflowID,
		},
	}
}

func ToParentMetadata(info *handler.DynamicNodeInfo) *event.ParentTaskExecutionMetadata {
	if info == nil || info.ParentTaskID == nil {
		return nil
	}
	return &event.ParentTaskExecutionMetadata{
		Id: info.ParentTaskID,
	}
}

func ToNodeExecEventPhase(p handler.EPhase) core.NodeExecution_Phase {
	switch p {
	case handler.EPhaseQueued:
		return core.NodeExecution_QUEUED
	case handler.EPhaseRunning, handler.EPhaseRetryableFailure:
		return core.NodeExecution_RUNNING
	case handler.EPhaseSkip:
		return core.NodeExecution_SKIPPED
	case handler.EPhaseSuccess:
		return core.NodeExecution_SUCCEEDED
	case handler.EPhaseFailed:
		return core.NodeExecution_FAILED
	default:
		return core.NodeExecution_UNDEFINED
	}
}

func ToNodeExecutionEvent(nodeExecID *core.NodeExecutionIdentifier, info handler.PhaseInfo, reader io.InputReader) (*event.NodeExecutionEvent, error) {
	if info.Phase == handler.EPhaseNotReady {
		return nil, nil
	}
	if info.Phase == handler.EPhaseUndefined {
		return nil, fmt.Errorf("illegal state, undefined phase received for node [%s]", nodeExecID.NodeId)
	}
	occurredTime, err := ptypes.TimestampProto(info.OccurredAt)
	if err != nil {
		return nil, err
	}

	nev := &event.NodeExecutionEvent{
		Id:         nodeExecID,
		Phase:      ToNodeExecEventPhase(info.Phase),
		InputUri:   reader.GetInputPath().String(),
		OccurredAt: occurredTime,
	}

	if info.Info != nil {
		nev.ParentTaskMetadata = ToParentMetadata(info.Info.DynamicNodeInfo)
		nev.TargetMetadata = ToNodeExecTargetMetadata(info.Info.WorkflowNodeInfo)

		if info.Err != nil {
			nev.OutputResult = &event.NodeExecutionEvent_Error{
				Error: info.Err,
			}
		} else if info.Info.OutputInfo != nil {
			nev.OutputResult = ToNodeExecOutput(info.Info.OutputInfo)
		}
	}
	return nev, nil
}

func ToNodePhase(p handler.EPhase) (v1alpha1.NodePhase, error) {
	switch p {
	case handler.EPhaseNotReady:
		return v1alpha1.NodePhaseNotYetStarted, nil
	case handler.EPhaseQueued:
		return v1alpha1.NodePhaseQueued, nil
	case handler.EPhaseRunning:
		return v1alpha1.NodePhaseRunning, nil
	case handler.EPhaseRetryableFailure:
		return v1alpha1.NodePhaseRetryableFailure, nil
	case handler.EPhaseSkip:
		return v1alpha1.NodePhaseSkipped, nil
	case handler.EPhaseSuccess:
		return v1alpha1.NodePhaseSucceeding, nil
	case handler.EPhaseFailed:
		return v1alpha1.NodePhaseFailing, nil
	}
	return v1alpha1.NodePhaseNotYetStarted, fmt.Errorf("no know conversion from handlerPhase[%d] to NodePhase", p)
}

func ToK8sTime(t time.Time) v1.Time {
	return v1.Time{Time: t}
}

func ToError(executionError *core.ExecutionError, reason string) string {
	if executionError != nil {
		return fmt.Sprintf("[%s]: %s", executionError.Code, executionError.Message)
	}
	if reason != "" {
		return reason
	}
	return "unknown error"
}

func UpdateNodeStatus(np v1alpha1.NodePhase, p handler.PhaseInfo, n *nodeStateManager, s v1alpha1.ExecutableNodeStatus) {
	// We update the phase only if it is not already updated
	if np != s.GetPhase() {
		s.UpdatePhase(np, ToK8sTime(p.OccurredAt), ToError(p.Err, p.Reason))
	}
	// Update TaskStatus
	if n.t != nil {
		t := s.GetOrCreateTaskStatus()
		t.SetPhaseVersion(n.t.PluginPhaseVersion)
		t.SetPhase(int(n.t.PluginPhase))
		t.SetPluginState(t.GetPluginState())
		t.SetPluginStateVersion(t.GetPluginStateVersion())

	} else if s.GetTaskNodeStatus() != nil {
		s.ClearTaskStatus()
	}

	// Update dynamic node status
	if n.d != nil {
		t := s.GetOrCreateDynamicNodeStatus()
		t.SetDynamicNodePhase(n.d.Phase)
	} else {
		s.ClearDynamicNodeStatus()
	}

	// Update branch node status
	if n.b != nil {
		t := s.GetOrCreateBranchStatus()
		if n.b.Phase == v1alpha1.BranchNodeError {
			t.SetBranchNodeError()
		} else if n.b.FinalizedNodeID != nil {
			t.SetBranchNodeSuccess(*n.b.FinalizedNodeID)
		} else {
			logger.Warnf(context.TODO(), "branch node status neither success nor error set")
		}
	}
}
