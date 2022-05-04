package gate

import (
	"context"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"

	//"github.com/flyteorg/flytepropeller/pkg/controller/nodes/recovery"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/flyteorg/flytestdlib/logger"
	//"github.com/flyteorg/flytestdlib/promutils/labeled"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/errors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
)

type gateNodeHandler struct {
	metrics      metrics
}

type metrics struct {
	//CacheError labeled.Counter
}

func newMetrics(scope promutils.Scope) metrics {
	return metrics{
		//CacheError: labeled.NewCounter("cache_err", "workflow handler failed to store or load from data store.", scope),
	}
}

func (g *gateNodeHandler) FinalizeRequired() bool {
	return false
}

func (g *gateNodeHandler) Setup(_ context.Context, _ handler.SetupContext) error {
	return nil
}

func (g *gateNodeHandler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {
	gateNode := nCtx.Node().GetGateNode()

	// retrieve gate node status?
	//gateNodeState := nCtx.NodeStateReader().GetGateNodeState()
	
	logger.Debug(ctx, "starting gate node %+v", gateNode)
	switch gateNode.GetKind() {
	case v1alpha1.ConditionalKindSignal:
		// TODO - handle
	case v1alpha1.ConditionalKindSleep:
		// retrieve sleep duration
		sleepConditional := gateNode.GetSleep()
		if sleepConditional == nil {
			errMsg := "gateNode sleep conditional is nil"
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM,
				errors.BadSpecificationError, errMsg, nil)), nil
		}

		sleepDuration := sleepConditional.GetDuration().AsDuration()

		// check duration of node sleep
		now := time.Now()
		startedAt := nCtx.NodeStatus().GetLastAttemptStartedAt().Time
		if sleepDuration <= now.Sub(startedAt) {
			// TODO - return sleeping!
		}
	default:
		errMsg := "gateNode does not have a supported conditional reference"
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM,
			errors.BadSpecificationError, errMsg, nil)), nil
	}

	/*// update gate node status?
	err := nCtx.NodeStateWriter().PutGateNodeState(handler.GateNodeState{
		Phase: v1alpha1.GateNodePhaseExecuting,
	})
	if err != nil {
		// TODO - handle:
	}*/

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})), nil

	/*logger.Debug(ctx, "Starting workflow Node")
	invalidWFNodeError := func() (handler.Transition, error) {
		errMsg := "workflow wfNode does not have a subworkflow or child workflow reference"
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM,
			errors.BadSpecificationError, errMsg, nil)), nil
	}

	updateNodeStateFn := func(transition handler.Transition, newPhase v1alpha1.WorkflowNodePhase, err error) (handler.Transition, error) {
		if err != nil {
			return transition, err
		}

		workflowNodeState := handler.WorkflowNodeState{Phase: newPhase}
		err = nCtx.NodeStateWriter().PutWorkflowNodeState(workflowNodeState)
		if err != nil {
			logger.Errorf(ctx, "Failed to store WorkflowNodeState, err :%s", err.Error())
			return handler.UnknownTransition, err
		}

		return transition, err
	}

	wfNode := nCtx.Node().GetWorkflowNode()
	wfNodeState := nCtx.NodeStateReader().GetWorkflowNodeState()
	workflowPhase := wfNodeState.Phase
	if workflowPhase == v1alpha1.WorkflowNodePhaseUndefined {
		if wfNode == nil {
			errMsg := "Invoked workflow handler, for a non workflow Node."
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM, errors.RuntimeExecutionError, errMsg, nil)), nil
		}

		if wfNode.GetSubWorkflowRef() != nil {
			trns, err := w.subWfHandler.StartSubWorkflow(ctx, nCtx)
			return updateNodeStateFn(trns, v1alpha1.WorkflowNodePhaseExecuting, err)
		} else if wfNode.GetLaunchPlanRefID() != nil {
			trns, err := w.lpHandler.StartLaunchPlan(ctx, nCtx)
			return updateNodeStateFn(trns, v1alpha1.WorkflowNodePhaseExecuting, err)
		}

		return invalidWFNodeError()
	} else if workflowPhase == v1alpha1.WorkflowNodePhaseExecuting {
		if wfNode.GetSubWorkflowRef() != nil {
			return w.subWfHandler.CheckSubWorkflowStatus(ctx, nCtx)
		} else if wfNode.GetLaunchPlanRefID() != nil {
			return w.lpHandler.CheckLaunchPlanStatus(ctx, nCtx)
		}
	} else if workflowPhase == v1alpha1.WorkflowNodePhaseFailing {
		if wfNode == nil {
			errMsg := "Invoked workflow handler, for a non workflow Node."
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM, errors.RuntimeExecutionError, errMsg, nil)), nil
		}

		if wfNode.GetSubWorkflowRef() != nil {
			trns, err := w.subWfHandler.HandleFailingSubWorkflow(ctx, nCtx)
			return updateNodeStateFn(trns, workflowPhase, err)
		} else if wfNode.GetLaunchPlanRefID() != nil {
			// There is no failure node for launch plans, terminate immediately.
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailureErr(wfNodeState.Error, nil)), nil
		}

		return invalidWFNodeError()
	}*/

	// TODO hamersaw - update
	errMsg := "workflow wfNode does not have a subworkflow or child workflow reference"
	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM,
		errors.BadSpecificationError, errMsg, nil)), nil
}

func (b *gateNodeHandler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext, reason string) error {
	/*wfNode := nCtx.Node().GetWorkflowNode()
	if wfNode.GetSubWorkflowRef() != nil {
		return w.subWfHandler.HandleAbort(ctx, nCtx, reason)
	}

	if wfNode.GetLaunchPlanRefID() != nil {
		return w.lpHandler.HandleAbort(ctx, nCtx, reason)
	}*/
	return nil
}

func (w *gateNodeHandler) Finalize(ctx context.Context, _ handler.NodeExecutionContext) error {
	//logger.Warnf(ctx, "Subworkflow finalize invoked. Nothing to be done")
	return nil
}

func New(eventConfig *config.EventConfig, scope promutils.Scope) handler.Node {
	gateScope := scope.NewSubScope("gate")
	return &gateNodeHandler{
		metrics: newMetrics(gateScope),
	}
}
