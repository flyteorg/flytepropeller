package subworkflow

import (
	"context"
	"fmt"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	errors2 "github.com/pkg/errors"
)

type launchPlanHandler struct {
	launchPlan launchplan.Executor
}

func (l *launchPlanHandler) StartLaunchPlan(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {
	nodeInputs, err := nCtx.InputReader().Get(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to read input. Error [%s]", err)
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RuntimeExecutionError), errMsg, nil)), nil
	}

	w := nCtx.Workflow()
	nodeStatus := w.GetNodeExecutionStatus(nCtx.NodeID())
	childID, err := GetChildWorkflowExecutionID(
		w.GetExecutionID().WorkflowExecutionIdentifier,
		nCtx.NodeID(),
		nodeStatus.GetAttempts(),
	)
	if err != nil {
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RuntimeExecutionError), "failed to create unique ID", nil)), nil
	}

	launchCtx := launchplan.LaunchContext{
		// TODO we need to add principal and nestinglevel as annotations or labels?
		Principal:    "unknown",
		NestingLevel: 0,
		ParentNodeExecution: &core.NodeExecutionIdentifier{
			NodeId:      nCtx.NodeID(),
			ExecutionId: w.GetExecutionID().WorkflowExecutionIdentifier,
		},
	}
	err = l.launchPlan.Launch(ctx, launchCtx, childID, nCtx.Node().GetWorkflowNode().GetLaunchPlanRefID().Identifier, nodeInputs)
	if err != nil {
		if launchplan.IsAlreadyExists(err) {
			logger.Info(ctx, "Execution already exists [%s].", childID.Name)
		} else if launchplan.IsUserError(err) {
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RuntimeExecutionError), "failed to create unique ID", nil)), nil
		} else {
			return handler.UnknownTransition, err
		}
	} else {
		logger.Infof(ctx, "Launched launchplan with ID [%s]", childID.Name)
	}

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
}

func (l *launchPlanHandler) CheckLaunchPlanStatus(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {

	// Handle launch plan
	w := nCtx.Workflow()
	nodeStatus := w.GetNodeExecutionStatus(nCtx.NodeID())
	childID, err := GetChildWorkflowExecutionID(
		w.GetExecutionID().WorkflowExecutionIdentifier,
		nCtx.NodeID(),
		nodeStatus.GetAttempts(),
	)

	if err != nil {
		// THIS SHOULD NEVER HAPPEN
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RuntimeExecutionError), "failed to create unique ID", nil)), nil
	}

	wfStatusClosure, err := l.launchPlan.GetStatus(ctx, childID)
	if err != nil {
		if launchplan.IsNotFound(err) { //NotFound
			errorCode, _ := errors.GetErrorCode(err)
			err = errors.Wrapf(errorCode, nCtx.NodeID(), err, "launch-plan not found")
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errorCode), err.Error(), nil)), nil
		}

		return handler.UnknownTransition, err
	}

	if wfStatusClosure == nil {
		logger.Info(ctx, "Retrieved Launch Plan status is nil. This might indicate pressure on the admin cache."+
			" Consider tweaking its size to allow for more concurrent executions to be cached.")
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
	}

	var wErr error
	switch wfStatusClosure.GetPhase() {
	case core.WorkflowExecution_ABORTED:
		wErr = fmt.Errorf("launchplan execution aborted")
		err = errors.Wrapf(errors.RemoteChildWorkflowExecutionFailed, nCtx.NodeID(), wErr, "launchplan [%s] failed", childID.Name)
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RemoteChildWorkflowExecutionFailed), err.Error(), nil)), nil
	case core.WorkflowExecution_FAILED:
		errMsg := fmt.Sprintf("launchplan execution failed without explicit error")
		if wfStatusClosure.GetError() != nil {
			errMsg = fmt.Sprintf(" errorCode[%s]: %s", wfStatusClosure.GetError().Code, wfStatusClosure.GetError().Message)
		}
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RemoteChildWorkflowExecutionFailed), errMsg, nil)), nil
	case core.WorkflowExecution_SUCCEEDED:
		if wfStatusClosure.GetOutputs() != nil {
			outputFile := v1alpha1.GetOutputsFile(nodeStatus.GetDataDir())
			childOutput := &core.LiteralMap{}
			uri := wfStatusClosure.GetOutputs().GetUri()
			store := nCtx.DataStore()

			if uri != "" {
				// Copy remote data to local S3 path
				if err := store.ReadProtobuf(ctx, storage.DataReference(uri), childOutput); err != nil {
					if storage.IsNotFound(err) {
						errMsg := fmt.Sprintf("remote output for launchplan execution was not found, uri [%s], err %s", uri, err.Error())
						return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RemoteChildWorkflowExecutionFailed), errMsg, nil)), nil
					}
					err := errors2.Wrapf(err, "failed to read outputs from child workflow @ [%s]", uri)
					return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoUndefined), err
				}
			} else if wfStatusClosure.GetOutputs().GetValues() != nil {
				// Store data to S3Path
				childOutput = wfStatusClosure.GetOutputs().GetValues()
			}
			if err := store.WriteProtobuf(ctx, outputFile, storage.Options{}, childOutput); err != nil {
				logger.Debugf(ctx, "failed to write data to Storage, err: %v", err.Error())
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoUndefined), errors.Wrapf(errors.CausedByError, nCtx.NodeID(), err, "failed to copy outputs for child workflow")
			}
		}
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
	}
	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
}

func (l *launchPlanHandler) HandleAbort(ctx context.Context, w v1alpha1.ExecutableWorkflow, node v1alpha1.ExecutableNode) error {
	nodeStatus := w.GetNodeExecutionStatus(node.GetID())
	childID, err := GetChildWorkflowExecutionID(
		w.GetExecutionID().WorkflowExecutionIdentifier,
		node.GetID(),
		nodeStatus.GetAttempts(),
	)
	if err != nil {
		// THIS SHOULD NEVER HAPPEN
		return err
	}
	return l.launchPlan.Kill(ctx, childID, fmt.Sprintf("parent execution id [%s] aborted", w.GetName()))
}
