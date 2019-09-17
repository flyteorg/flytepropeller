package branch

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
)

type branchHandler struct {
	nodeExecutor executors.Node
}

func (b *branchHandler) FinalizeRequired() bool {
	return true
}

func (b *branchHandler) Setup(ctx context.Context, setupContext handler.SetupContext) error {
	logger.Debugf(ctx, "BranchNode::Setup: nothing to do")
	return nil
}

func (b *branchHandler) transitionToFailure(code string, errorMsg string) handler.Transition {
	err := &core.ExecutionError{
		Code:    string(errors.IllegalStateError),
		Message: "invoked branch handler for a non branch node.",
	}
	phase := handler.PhaseInfo{
		Phase:      handler.EPhaseFailed,
		Err:        err,
		OccurredAt: time.Now(),
		Info:       nil,
		Reason:     "invalid branch node",
	}
	transition := handler.DoTransition(handler.TransitionTypeEphemeral, phase)
	return transition
}

func (b *branchHandler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {

	logger.Debug(ctx, "Starting Branch Node")
	branchNode := nCtx.Node().GetBranchNode()
	w := nCtx.Workflow()

	nodeInputs, err := nCtx.InputReader().Get(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to read input. Error [%s]", err)
		return b.transitionToFailure(string(errors.RuntimeExecutionError), errMsg), nil
	}

	if nCtx.NodeStatus().GetPhase() == v1alpha1.NodePhaseNotYetStarted {
		finalNode, err := DecideBranch(ctx, w, nCtx.NodeID(), branchNode, nodeInputs)
		if err != nil {
			errMsg := fmt.Sprintf("Branch evaluation failed. Error [%s]", err)
			return b.transitionToFailure(string(errors.IllegalStateError), errMsg), nil
		}

		branchNodeState := handler.BranchNodeState{FinalizedNodeID: finalNode, Phase: v1alpha1.BranchNodeSuccess}
		err = nCtx.NodeStateWriter().PutBranchNode(branchNodeState)
		if err != nil {
			logger.Errorf(ctx, "Failed to store TaskNode state, err :%s", err.Error())
			return handler.UnknownTransition, err
		}

		var ok bool
		childNode, ok := w.GetNode(*finalNode)
		if !ok {
			logger.Debugf(ctx, "Branch downstream finalized node not found. FinalizedNode [%s]", *finalNode)
			errMsg := fmt.Sprintf("Branch downstream finalized node not found. FinalizedNode [%s]", *finalNode)
			logger.Debugf(ctx, errMsg)
			return b.transitionToFailure(string(errors.DownstreamNodeNotFoundError), errMsg), nil
		}
		i := nCtx.NodeID()
		childNodeStatus := w.GetNodeExecutionStatus(childNode.GetID())
		childNodeStatus.SetParentNodeID(&i)

		logger.Debugf(ctx, "Recursing down branchNodestatus node")

		nodeStatus := w.GetNodeExecutionStatus(nCtx.NodeID())
		return b.recurseDownstream(ctx, nCtx, nodeStatus, childNode)

	}

	// If the branchNodestatus was already evaluated i.e, Node is in Running status
	nodeStatus := w.GetNodeExecutionStatus(nCtx.NodeID())
	branchStatus := nodeStatus.GetOrCreateBranchStatus()
	userError := branchNode.GetElseFail()
	finalNodeID := branchStatus.GetFinalizedNode()
	if finalNodeID == nil {
		if userError != nil {
			// We should never reach here, but for safety and completeness
			errMsg := fmt.Sprintf("Branch node userError [%s]", userError)
			return b.transitionToFailure(string(errors.UserProvidedError), errMsg), nil
		}

		err := &core.ExecutionError{
			Code:    string(errors.IllegalStateError),
			Message: "no node finalized through previous branchNodestatus evaluation.",
		}
		phase := handler.PhaseInfo{
			Phase:      handler.EPhaseRunning,
			Err:        err,
			OccurredAt: time.Now(),
			Info:       nil,
		}
		return handler.DoTransition(handler.TransitionTypeEphemeral, phase), nil
	}
	var ok bool
	branchTakenNode, ok := w.GetNode(*finalNodeID)
	if !ok {
		errMsg := fmt.Sprintf("Downstream node [%v] not found", *finalNodeID)
		return b.transitionToFailure(string(errors.DownstreamNodeNotFoundError), errMsg), nil
	}
	// Recurse downstream
	return b.recurseDownstream(ctx, nCtx, nodeStatus, branchTakenNode)
}

func (b *branchHandler) recurseDownstream(ctx context.Context, nCtx handler.NodeExecutionContext, nodeStatus v1alpha1.ExecutableNodeStatus, branchTakenNode v1alpha1.ExecutableNode) (handler.Transition, error) {
	w := nCtx.Workflow()

	downstreamStatus, err := b.nodeExecutor.RecursiveNodeHandler(ctx, w, branchTakenNode)
	if err != nil {
		errMsg := fmt.Sprintf("executing branch downstream node failed. Error [%s]", err)
		code, _ := errors.GetErrorCode(err)
		return b.transitionToFailure(string(code), errMsg), nil
	}

	if downstreamStatus.IsComplete() {
		// For branch node we set the output node to be the same as the child nodes output
		childNodeStatus := w.GetNodeExecutionStatus(branchTakenNode.GetID())
		nodeStatus.SetDataDir(childNodeStatus.GetDataDir())

		phase := handler.PhaseInfo{
			Phase:      handler.EPhaseSuccess,
			Err:        nil,
			OccurredAt: time.Now(),
			Info:       nil,
		}
		return handler.DoTransition(handler.TransitionTypeEphemeral, phase), nil
	}

	if downstreamStatus.HasFailed() {
		errMsg := downstreamStatus.Err.Error()
		code, _ := errors.GetErrorCode(downstreamStatus.Err)
		return b.transitionToFailure(string(code), errMsg), nil
	}

	phase := handler.PhaseInfo{
		Phase:      handler.EPhaseRunning,
		Err:        nil,
		OccurredAt: time.Now(),
		Info:       nil,
	}
	return handler.DoTransition(handler.TransitionTypeEphemeral, phase), nil
}

func (b *branchHandler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext) error {

	branch := nCtx.Node().GetBranchNode()
	w := nCtx.Workflow()
	if branch == nil {
		return errors.Errorf(errors.IllegalStateError, w.GetID(), nCtx.NodeID(), "Invoked branch handler, for a non branch node.")
	}
	// If the branch was already evaluated i.e, Node is in Running status
	userError := branch.GetElseFail()
	finalNodeID := nCtx.NodeStateReader().GetBranchNode().FinalizedNodeID
	if finalNodeID == nil {
		if userError != nil {
			// We should never reach here, but for safety and completeness
			return errors.Errorf(errors.UserProvidedError, nCtx.NodeID(), userError.Message)
		}
		return errors.Errorf(errors.IllegalStateError, nCtx.NodeID(), "No node finalized through previous branch evaluation.")
	}

	var ok bool
	branchTakenNode, ok := w.GetNode(*finalNodeID)
	if !ok {
		return errors.Errorf(errors.DownstreamNodeNotFoundError, w.GetID(), nCtx.NodeID(), "Downstream node [%v] not found", *finalNodeID)
	}
	// Recurse downstream
	return b.nodeExecutor.AbortHandler(ctx, w, branchTakenNode)
}

func (b *branchHandler) Finalize(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	logger.Debugf(ctx, "BranchNode::Finalizer: nothing to do")
	return nil
}

func New(executor executors.Node, scope promutils.Scope) handler.Node {
	//branchScope := scope.NewSubScope("branch")
	return &branchHandler{
		nodeExecutor: executor,
	}
}
