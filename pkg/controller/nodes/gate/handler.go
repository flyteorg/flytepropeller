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
	gateNodeState := nCtx.NodeStateReader().GetGateNodeState()

	if gateNodeState.Phase == v1alpha1.GateNodePhaseUndefined {
		gateNodeState.Phase = v1alpha1.GateNodePhaseExecuting
		gateNodeState.StartedAt = time.Now()
	}
	
	logger.Debug(ctx, "starting gate node %+v", gateNode)
	switch gateNode.GetKind() {
	case v1alpha1.ConditionKindSignal:
		// TODO - handle
	case v1alpha1.ConditionKindSleep:
		logger.Infof(ctx, "HAMERSAW: accessing sleep conditional")
		// retrieve sleep duration
		sleepCondition := gateNode.GetSleep()
		if sleepCondition == nil {
			errMsg := "gateNode sleep conditional is nil"
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM,
				errors.BadSpecificationError, errMsg, nil)), nil
		}

		sleepDuration := sleepCondition.GetDuration().AsDuration()

		// check duration of node sleep
		now := time.Now()
		logger.Infof(ctx, "HAMERSAW: %+v %s %v", gateNodeState.StartedAt, now.Sub(gateNodeState.StartedAt), sleepDuration <= now.Sub(gateNodeState.StartedAt))
		if sleepDuration <= now.Sub(gateNodeState.StartedAt) {
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(&handler.ExecutionInfo{})), nil
		}
	default:
		errMsg := "gateNode does not have a supported conditional reference"
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM,
			errors.BadSpecificationError, errMsg, nil)), nil
	}

	// update gate node status?
	if err := nCtx.NodeStateWriter().PutGateNodeState(gateNodeState); err != nil {
		logger.Errorf(ctx, "Failed to store TaskNode state, err :%s", err.Error())
		return handler.UnknownTransition, err
	}

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})), nil
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
