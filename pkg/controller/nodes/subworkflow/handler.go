package subworkflow

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytestdlib/promutils/labeled"
)

type workflowNodeHandler struct {
	lpHandler    launchPlanHandler
	subWfHandler subworkflowHandler
	metrics      metrics
}

type metrics struct {
	CacheError labeled.Counter
}

func newMetrics(scope promutils.Scope) metrics {
	return metrics{
		CacheError: labeled.NewCounter("cache_err", "workflow handler failed to store or load from data store.", scope),
	}
}

func (w *workflowNodeHandler) FinalizeRequired() bool {
	return false
}

func (w *workflowNodeHandler) Setup(ctx context.Context, setupContext handler.SetupContext) error {
	return nil
}

func (w *workflowNodeHandler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {

	wf := nCtx.Workflow()

	logger.Debug(ctx, "Starting workflow Node")
	wfNode := nCtx.Node().GetWorkflowNode()

	if wfNode == nil {
		errMsg := "Invoked workflow handler, for a non workflow Node."
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.RuntimeExecutionError), errMsg, nil)), nil
	}

	if wfNode.GetSubWorkflowRef() != nil {
		return w.subWfHandler.StartSubWorkflow(ctx, wf, nCtx)
	}

	if nCtx.Node().GetWorkflowNode().GetLaunchPlanRefID() != nil {
		return w.lpHandler.StartLaunchPlan(ctx, nCtx)
	}

	status := wf.GetNodeExecutionStatus(nCtx.NodeID())
	if wfNode.GetSubWorkflowRef() != nil {
		return w.subWfHandler.CheckSubWorkflowStatus(ctx, nCtx, wf, status)
	}

	if wfNode.GetLaunchPlanRefID() != nil {
		return w.lpHandler.CheckLaunchPlanStatus(ctx, nCtx)
	}

	errMsg := "workflow wfNode does not have a subworkflow or child workflow reference"
	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(string(errors.BadSpecificationError), errMsg, nil)), nil
}

func (w *workflowNodeHandler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext, reason string) error {
	wf := nCtx.Workflow()
	if nCtx.Node().GetWorkflowNode().GetSubWorkflowRef() != nil {
		return w.subWfHandler.HandleAbort(ctx, wf, nCtx.Node())
	}

	if nCtx.Node().GetWorkflowNode().GetLaunchPlanRefID() != nil {
		return w.lpHandler.HandleAbort(ctx, wf, nCtx.Node())
	}
	return nil
}

func (w *workflowNodeHandler) Finalize(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	logger.Debugf(ctx, "WorkflowNode::Finalizer: nothing to do")
	return nil
}

func New(executor executors.Node, workflowLauncher launchplan.Executor, scope promutils.Scope) handler.Node {
	workflowScope := scope.NewSubScope("workflow")
	return &workflowNodeHandler{
		subWfHandler: newSubworkflowHandler(executor),
		lpHandler: launchPlanHandler{
			launchPlan: workflowLauncher,
		},
		metrics: newMetrics(workflowScope),
	}
}
