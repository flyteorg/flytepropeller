package nodes

import (
	"context"
	"fmt"
	"time"

	errors2 "github.com/lyft/flytestdlib/errors"

	"github.com/golang/protobuf/ptypes"
	"github.com/lyft/flyteidl/clients/go/events"
	eventsErr "github.com/lyft/flyteidl/clients/go/events/errors"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/catalog"
	"github.com/lyft/flytepropeller/pkg/controller/config"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/lyft/flytestdlib/storage"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type nodeMetrics struct {
	Scope              promutils.Scope
	FailureDuration    labeled.StopWatch
	SuccessDuration    labeled.StopWatch
	ResolutionFailure  labeled.Counter
	InputsWriteFailure labeled.Counter
	TimedOutFailure    labeled.Counter

	// Measures the latency between the last parent node stoppedAt time and current node's queued time.
	TransitionLatency labeled.StopWatch
	// Measures the latency between the time a node's been queued to the time the handler reported the executable moved
	// to running state
	QueuingLatency         labeled.StopWatch
	NodeExecutionTime      labeled.StopWatch
	NodeInputGatherLatency labeled.StopWatch
}

type nodeExecutor struct {
	nodeHandlerFactory       HandlerFactory
	enqueueWorkflow          v1alpha1.EnqueueWorkflow
	store                    *storage.DataStore
	nodeRecorder             events.NodeEventRecorder
	taskRecorder             events.TaskEventRecorder
	metrics                  *nodeMetrics
	maxDatasetSizeBytes      int64
	outputResolver           OutputResolver
	defaultExecutionDeadline time.Duration
	defaultActiveDeadline    time.Duration
}

func (c *nodeExecutor) RecordTransitionLatency(ctx context.Context, w v1alpha1.ExecutableWorkflow, node v1alpha1.ExecutableNode, nodeStatus v1alpha1.ExecutableNodeStatus) {
	if nodeStatus.GetPhase() == v1alpha1.NodePhaseNotYetStarted || nodeStatus.GetPhase() == v1alpha1.NodePhaseQueued {
		// Log transition latency (The most recently finished parent node endAt time to this node's queuedAt time -now-)
		t, err := GetParentNodeMaxEndTime(ctx, w, node)
		if err != nil {
			logger.Warnf(ctx, "Failed to record transition latency for node. Error: %s", err.Error())
			return
		}
		if !t.IsZero() {
			c.metrics.TransitionLatency.Observe(ctx, t.Time, time.Now())
		}
	} else if nodeStatus.GetPhase() == v1alpha1.NodePhaseRetryableFailure && nodeStatus.GetLastUpdatedAt() != nil {
		c.metrics.TransitionLatency.Observe(ctx, nodeStatus.GetLastUpdatedAt().Time, time.Now())
	}
}

func (c *nodeExecutor) IdempotentRecordEvent(ctx context.Context, nodeEvent *event.NodeExecutionEvent) error {
	if nodeEvent == nil {
		return fmt.Errorf("event recording attempt of Nil Node execution event")
	}

	if nodeEvent.Id == nil {
		return fmt.Errorf("event recording attempt of with nil node Event ID")
	}

	logger.Infof(ctx, "Recording event p[%+v]", nodeEvent)
	err := c.nodeRecorder.RecordNodeEvent(ctx, nodeEvent)
	if err != nil {
		if nodeEvent.GetId().NodeId == v1alpha1.EndNodeID {
			return nil
		}

		if eventsErr.IsAlreadyExists(err) {
			logger.Infof(ctx, "Node event phase: %s, nodeId %s already exist",
				nodeEvent.Phase.String(), nodeEvent.GetId().NodeId)
			return nil
		} else if eventsErr.IsEventAlreadyInTerminalStateError(err) {
			logger.Warningf(ctx, "Failed to record nodeEvent, error [%s]", err.Error())
			return errors.Wrapf(errors.IllegalStateError, nodeEvent.Id.NodeId, err, "phase mis-match mismatch between propeller and control plane; Trying to record Node p: %s", nodeEvent.Phase)
		}
	}
	return err
}

// In this method we check if the queue is ready to be processed and if so, we prime it in Admin as queued
// Before we start the node execution, we need to transition this Node status to Queued.
// This is because a node execution has to exist before task/wf executions can start.
func (c *nodeExecutor) preExecute(ctx context.Context, w v1alpha1.ExecutableWorkflow, node v1alpha1.ExecutableNode, nodeStatus v1alpha1.ExecutableNodeStatus) (handler.PhaseInfo, error) {
	logger.Debugf(ctx, "Node not yet started")
	// Query the nodes information to figure out if it can be executed.
	predicatePhase, err := CanExecute(ctx, w, node)
	if err != nil {
		logger.Debugf(ctx, "Node failed in CanExecute. Error [%s]", err)
		return handler.PhaseInfoUndefined, err
	}

	if predicatePhase == PredicatePhaseReady {

		// TODO: Performance problem, we maybe in a retry loop and do not need to resolve the inputs again.
		// For now we will do this.
		dataDir := nodeStatus.GetDataDir()
		var nodeInputs *core.LiteralMap
		if !node.IsStartNode() {
			t := c.metrics.NodeInputGatherLatency.Start(ctx)
			defer t.Stop()
			// Can execute
			var err error
			nodeInputs, err = Resolve(ctx, c.outputResolver, w, node.GetID(), node.GetInputBindings())
			// TODO we need to handle retryable, network errors here!!
			if err != nil {
				c.metrics.ResolutionFailure.Inc(ctx)
				logger.Warningf(ctx, "Failed to resolve inputs for Node. Error [%v]", err)
				return handler.PhaseInfoFailure("BindingResolutionFailure", err.Error(), nil), nil
			}

			if nodeInputs != nil {
				inputsFile := v1alpha1.GetInputsFile(dataDir)
				if err := c.store.WriteProtobuf(ctx, inputsFile, storage.Options{}, nodeInputs); err != nil {
					c.metrics.InputsWriteFailure.Inc(ctx)
					logger.Errorf(ctx, "Failed to store inputs for Node. Error [%v]. InputsFile [%s]", err, inputsFile)
					return handler.PhaseInfoUndefined, errors.Wrapf(
						errors.StorageError, node.GetID(), err, "Failed to store inputs for Node. InputsFile [%s]", inputsFile)
				}
			}

			logger.Debugf(ctx, "Node Data Directory [%s].", nodeStatus.GetDataDir())
		}
		return handler.PhaseInfoQueued("node queued"), nil
	}
	// Now that we have resolved the inputs, we can record as a transition latency. This is because we have completed
	// all the overhead that we have to compute. Any failures after this will incur this penalty, but it could be due
	// to various external reasons - like queuing, overuse of quota, plugin overhead etc.
	logger.Debugf(ctx, "preExecute completed in phase [%s]", predicatePhase.String())
	if predicatePhase == PredicatePhaseSkip {
		return handler.PhaseInfoSkip(nil, "Node Skipped as parent node was skipped"), nil
	}
	return handler.PhaseInfoNotReady("predecessor node not yet complete"), nil
}

func (c *nodeExecutor) isTimeoutExpired(queuedAt *metav1.Time, timeout time.Duration) bool {
	if !queuedAt.IsZero() && timeout != 0 {
		deadline := queuedAt.Add(timeout)
		if deadline.Before(time.Now()) {
			return true
		}
	}
	return false
}

func (c *nodeExecutor) execute(ctx context.Context, h handler.Node, nCtx *execContext, nodeStatus v1alpha1.ExecutableNodeStatus) (handler.PhaseInfo, error) {
	logger.Debugf(ctx, "Executing node")
	defer logger.Debugf(ctx, "Node execution round complete")

	t, err := h.Handle(ctx, nCtx)
	if err != nil {
		return handler.PhaseInfoUndefined, err
	}

	phase := t.Info().GetPhase()
	// check for timeout for non-terminal phases
	if !phase.IsTerminal() {
		activeDeadline := c.defaultActiveDeadline
		if nCtx.Node().GetActiveDeadline() != nil {
			activeDeadline = *nCtx.Node().GetActiveDeadline()
		}
		if c.isTimeoutExpired(nodeStatus.GetQueuedAt(), activeDeadline) {
			logger.Errorf(ctx, "Node has timed out; timeout configured: %v", activeDeadline)
			return handler.PhaseInfoTimedOut(nil, "active deadline elapsed"), nil
		}

		// Execution timeout is a retry-able error
		executionDeadline := c.defaultExecutionDeadline
		if nCtx.Node().GetExecutionDeadline() != nil {
			executionDeadline = *nCtx.Node().GetExecutionDeadline()
		}
		if c.isTimeoutExpired(nodeStatus.GetLastAttemptStartedAt(), executionDeadline) {
			logger.Errorf(ctx, "Current execution for the node timed out; timeout configured: %v", executionDeadline)
			return handler.PhaseInfoRetryableFailure("TimeOut", "node execution timed out", nil), nil
		}
	}

	if t.Info().GetPhase() == handler.EPhaseRetryableFailure {
		maxAttempts := uint32(0)
		if nCtx.Node().GetRetryStrategy() != nil && nCtx.Node().GetRetryStrategy().MinAttempts != nil {
			maxAttempts = uint32(*nCtx.Node().GetRetryStrategy().MinAttempts)
		}

		attempts := nodeStatus.GetAttempts() + 1
		if attempts >= maxAttempts {
			return handler.PhaseInfoFailure(
				fmt.Sprintf("RetriesExhausted|%s", t.Info().GetErr().Code),
				fmt.Sprintf("[%d/%d] attempts done. Last Error: %s", attempts, maxAttempts, t.Info().GetErr().Message),
				t.Info().GetInfo(),
			), nil
		}

		nodeStatus.IncrementAttempts()
		// Retrying to clearing all status
		nCtx.nsm.clearNodeStatus()
	}

	return t.Info(), nil
}

func (c *nodeExecutor) abort(ctx context.Context, h handler.Node, nCtx handler.NodeExecutionContext, reason string) error {
	logger.Debugf(ctx, "Calling aborting & finalize")
	if err := h.Abort(ctx, nCtx, reason); err != nil {
		return err
	}
	return h.Finalize(ctx, nCtx)

}

func (c *nodeExecutor) finalize(ctx context.Context, h handler.Node, nCtx handler.NodeExecutionContext) error {
	return h.Finalize(ctx, nCtx)
}

func (c *nodeExecutor) handleNode(ctx context.Context, w v1alpha1.ExecutableWorkflow, node v1alpha1.ExecutableNode) (executors.NodeStatus, error) {
	logger.Debugf(ctx, "Handling Node [%s]", node.GetID())
	defer logger.Debugf(ctx, "Completed node [%s]", node.GetID())

	nodeExecID := &core.NodeExecutionIdentifier{
		NodeId:      node.GetID(),
		ExecutionId: w.GetExecutionID().WorkflowExecutionIdentifier,
	}
	nodeStatus := w.GetNodeExecutionStatus(node.GetID())

	// Now depending on the node type decide
	h, err := c.nodeHandlerFactory.GetHandler(node.GetKind())
	if err != nil {
		return executors.NodeStatusUndefined, err
	}

	if len(nodeStatus.GetDataDir()) == 0 {
		// Predicate ready, lets Resolve the data
		dataDir, err := w.GetExecutionStatus().ConstructNodeDataDir(ctx, c.store, node.GetID())
		if err != nil {
			return executors.NodeStatusUndefined, err
		}

		nodeStatus.SetDataDir(dataDir)
	}

	nCtx, err := c.newNodeExecContextDefault(ctx, w, node, nodeStatus)
	if err != nil {
		return executors.NodeStatusUndefined, err
	}

	currentPhase := nodeStatus.GetPhase()

	// Optimization!
	// If it is start node we directly move it to Queued without needing to run preExecute
	if currentPhase == v1alpha1.NodePhaseNotYetStarted && !node.IsStartNode() {
		logger.Debugf(ctx, "Node not yet started, running pre-execute")
		defer logger.Debugf(ctx, "Node pre-execute completed")
		p, err := c.preExecute(ctx, w, node, nodeStatus)
		if err != nil {
			logger.Errorf(ctx, "failed preExecute for node. Error: %s", err.Error())
			return executors.NodeStatusUndefined, err
		}
		if p.GetPhase() == handler.EPhaseUndefined {
			return executors.NodeStatusUndefined, errors.Errorf(errors.IllegalStateError, node.GetID(), "received undefined phase.")
		}
		if p.GetPhase() == handler.EPhaseNotReady {
			return executors.NodeStatusPending, nil
		}

		np, err := ToNodePhase(p.GetPhase())
		if err != nil {
			return executors.NodeStatusUndefined, errors.Wrapf(errors.IllegalStateError, node.GetID(), err, "failed to move from queued")
		}
		if np != nodeStatus.GetPhase() {
			// assert np == Queued!
			logger.Infof(ctx, "Change in node state detected from [%s] -> [%s]", nodeStatus.GetPhase().String(), np.String())
			nev, err := ToNodeExecutionEvent(nodeExecID, p, nCtx.InputReader(), nCtx.NodeStatus())
			if err != nil {
				return executors.NodeStatusUndefined, errors.Wrapf(errors.IllegalStateError, node.GetID(), err, "could not convert phase info to event")
			}
			err = c.IdempotentRecordEvent(ctx, nev)
			if err != nil {
				logger.Warningf(ctx, "Failed to record nodeEvent, error [%s]", err.Error())
				return executors.NodeStatusUndefined, errors.Wrapf(errors.EventRecordingFailed, node.GetID(), err, "failed to record node event")
			}
			UpdateNodeStatus(np, p, nCtx.nsm, nodeStatus)
			c.RecordTransitionLatency(ctx, w, node, nodeStatus)
		}
		if np == v1alpha1.NodePhaseQueued {
			return executors.NodeStatusQueued, nil
		} else if np == v1alpha1.NodePhaseSkipped {
			return executors.NodeStatusSuccess, nil
		}
		return executors.NodeStatusPending, nil
	}

	if currentPhase == v1alpha1.NodePhaseFailing {
		logger.Debugf(ctx, "node failing")
		if err := c.finalize(ctx, h, nCtx); err != nil {
			return executors.NodeStatusUndefined, err
		}

		nodeStatus.UpdatePhase(v1alpha1.NodePhaseFailed, v1.Now(), nodeStatus.GetMessage())
		c.metrics.FailureDuration.Observe(ctx, nodeStatus.GetStartedAt().Time, nodeStatus.GetStoppedAt().Time)
		// TODO we need to have a way to find the error message from failing to failed!
		return executors.NodeStatusFailed(fmt.Errorf(nodeStatus.GetMessage())), nil
	}

	if currentPhase == v1alpha1.NodePhaseTimingOut {
		logger.Debugf(ctx, "node timing out")
		if err := c.abort(ctx, h, nCtx, "node timed out"); err != nil {
			return executors.NodeStatusUndefined, err
		}

		nodeStatus.UpdatePhase(v1alpha1.NodePhaseTimedOut, v1.Now(), nodeStatus.GetMessage())
		c.metrics.TimedOutFailure.Inc(ctx)
		return executors.NodeStatusTimedOut, nil
	}

	if currentPhase == v1alpha1.NodePhaseSucceeding {
		logger.Debugf(ctx, "node succeeding")
		if err := c.finalize(ctx, h, nCtx); err != nil {
			return executors.NodeStatusUndefined, err
		}

		nodeStatus.UpdatePhase(v1alpha1.NodePhaseSucceeded, v1.Now(), "completed successfully")
		c.metrics.SuccessDuration.Observe(ctx, nodeStatus.GetStartedAt().Time, nodeStatus.GetStoppedAt().Time)
		return executors.NodeStatusSuccess, nil
	}

	if currentPhase == v1alpha1.NodePhaseRetryableFailure {
		logger.Debugf(ctx, "node failed with retryable failure, finalizing")
		if err := c.finalize(ctx, h, nCtx); err != nil {
			return executors.NodeStatusUndefined, err
		}

		nodeStatus.UpdatePhase(v1alpha1.NodePhaseRunning, v1.Now(), "retrying")
		// We are going to retry in the next round, so we should clear all current state
		nodeStatus.ClearDynamicNodeStatus()
		nodeStatus.ClearTaskStatus()
		nodeStatus.ClearWorkflowStatus()
		nodeStatus.ClearLastAttemptStartedAt()
		return executors.NodeStatusPending, nil
	}

	if currentPhase == v1alpha1.NodePhaseFailed {
		// This should never happen
		return executors.NodeStatusFailed(fmt.Errorf(nodeStatus.GetMessage())), nil
	}

	if currentPhase == v1alpha1.NodePhaseFailed {
		// This should never happen
		return executors.NodeStatusSuccess, nil
	}

	// case v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseRetryableFailure:
	logger.Debugf(ctx, "node executing, current phase [%s]", currentPhase)
	defer logger.Debugf(ctx, "node execution completed")
	p, err := c.execute(ctx, h, nCtx, nodeStatus)
	if err != nil {
		logger.Errorf(ctx, "failed Execute for node. Error: %s", err.Error())
		return executors.NodeStatusUndefined, err
	}

	if p.GetPhase() == handler.EPhaseUndefined {
		return executors.NodeStatusUndefined, errors.Errorf(errors.IllegalStateError, node.GetID(), "received undefined phase.")
	}

	np, err := ToNodePhase(p.GetPhase())
	if err != nil {
		return executors.NodeStatusUndefined, errors.Wrapf(errors.IllegalStateError, node.GetID(), err, "failed to move from queued")
	}

	finalStatus := executors.NodeStatusRunning
	if np == v1alpha1.NodePhaseFailing && !h.FinalizeRequired() {
		logger.Infof(ctx, "Finalize not required, moving node to Failed")
		np = v1alpha1.NodePhaseFailed
		finalStatus = executors.NodeStatusFailed(fmt.Errorf(ToError(p.GetErr(), p.GetReason())))
	}

	if np == v1alpha1.NodePhaseTimingOut && !h.FinalizeRequired() {
		logger.Infof(ctx, "Finalize not required, moving node to TimedOut")
		np = v1alpha1.NodePhaseTimedOut
		finalStatus = executors.NodeStatusTimedOut
	}

	if np == v1alpha1.NodePhaseSucceeding && !h.FinalizeRequired() {
		logger.Infof(ctx, "Finalize not required, moving node to Succeeded")
		np = v1alpha1.NodePhaseSucceeded
		finalStatus = executors.NodeStatusSuccess
	}

	// If it is retryable failure, we do no want to send any events, as the node is essentially still running
	if np != nodeStatus.GetPhase() && np != v1alpha1.NodePhaseRetryableFailure {
		// assert np == skipped, succeeding or failing
		logger.Infof(ctx, "Change in node state detected from [%s] -> [%s], (handler phase [%s])", nodeStatus.GetPhase().String(), np.String(), p.GetPhase().String())
		nev, err := ToNodeExecutionEvent(nodeExecID, p, nCtx.InputReader(), nCtx.NodeStatus())
		if err != nil {
			return executors.NodeStatusUndefined, errors.Wrapf(errors.IllegalStateError, node.GetID(), err, "could not convert phase info to event")
		}

		err = c.IdempotentRecordEvent(ctx, nev)
		if err != nil {
			logger.Warningf(ctx, "Failed to record nodeEvent, error [%s]", err.Error())
			return executors.NodeStatusUndefined, errors.Wrapf(errors.EventRecordingFailed, node.GetID(), err, "failed to record node event")
		}

		if np == v1alpha1.NodePhaseRunning {
			if nodeStatus.GetQueuedAt() != nil && nodeStatus.GetStartedAt() != nil {
				c.metrics.QueuingLatency.Observe(ctx, nodeStatus.GetQueuedAt().Time, nodeStatus.GetStartedAt().Time)
			}
		}
	}

	UpdateNodeStatus(np, p, nCtx.nsm, nodeStatus)
	return finalStatus, nil
}

// The space search for the next node to execute is implemented like a DFS algorithm. handleDownstream visits all the nodes downstream from
// the currentNode. Visit a node is the RecursiveNodeHandler. A visit may be partial, complete or may result in a failure.
func (c *nodeExecutor) handleDownstream(ctx context.Context, w v1alpha1.ExecutableWorkflow, currentNode v1alpha1.ExecutableNode) (executors.NodeStatus, error) {
	logger.Debugf(ctx, "Handling downstream Nodes")
	// This node is success. Handle all downstream nodes
	downstreamNodes, err := w.FromNode(currentNode.GetID())
	if err != nil {
		logger.Debugf(ctx, "Error when retrieving downstream nodes. Error [%v]", err)
		return executors.NodeStatusFailed(err), nil
	}
	if len(downstreamNodes) == 0 {
		logger.Debugf(ctx, "No downstream nodes found. Complete.")
		return executors.NodeStatusComplete, nil
	}
	// If any downstream node is failed, fail, all
	// Else if all are success then success
	// Else if any one is running then Downstream is still running
	allCompleted := true
	partialNodeCompletion := false
	for _, downstreamNodeName := range downstreamNodes {
		downstreamNode, ok := w.GetNode(downstreamNodeName)
		if !ok {
			return executors.NodeStatusFailed(errors.Errorf(errors.BadSpecificationError, currentNode.GetID(), "Unable to find Downstream Node [%v]", downstreamNodeName)), nil
		}
		state, err := c.RecursiveNodeHandler(ctx, w, downstreamNode)
		if err != nil {
			return executors.NodeStatusUndefined, err
		}
		if state.HasFailed() {
			logger.Debugf(ctx, "Some downstream node has failed, %s", state.Err.Error())
			return state, nil
		}
		if state.HasTimedOut() {
			logger.Debugf(ctx, "Some downstream node has timedout")
			return state, nil
		}
		if !state.IsComplete() {
			allCompleted = false
		}

		if state.PartiallyComplete() {
			// This implies that one of the downstream nodes has completed and workflow is ready for propagation
			// We do not propagate in current cycle to make it possible to store the state between transitions
			partialNodeCompletion = true
		}
	}
	if allCompleted {
		logger.Debugf(ctx, "All downstream nodes completed")
		return executors.NodeStatusComplete, nil
	}
	if partialNodeCompletion {
		return executors.NodeStatusSuccess, nil
	}
	return executors.NodeStatusPending, nil
}

func (c *nodeExecutor) SetInputsForStartNode(ctx context.Context, w v1alpha1.BaseWorkflowWithStatus, inputs *core.LiteralMap) (executors.NodeStatus, error) {
	startNode := w.StartNode()
	if startNode == nil {
		return executors.NodeStatusFailed(errors.Errorf(errors.BadSpecificationError, v1alpha1.StartNodeID, "Start node not found")), nil
	}
	ctx = contextutils.WithNodeID(ctx, startNode.GetID())
	if inputs == nil {
		logger.Infof(ctx, "No inputs for the workflow. Skipping storing inputs")
		return executors.NodeStatusComplete, nil
	}
	// StartNode is special. It does not have any processing step. It just takes the workflow (or subworkflow) inputs and converts to its own outputs
	nodeStatus := w.GetNodeExecutionStatus(startNode.GetID())
	if nodeStatus.GetDataDir() == "" {
		return executors.NodeStatusUndefined, errors.Errorf(errors.IllegalStateError, startNode.GetID(), "no data-dir set, cannot store inputs")
	}
	outputFile := v1alpha1.GetOutputsFile(nodeStatus.GetDataDir())
	so := storage.Options{}
	if err := c.store.WriteProtobuf(ctx, outputFile, so, inputs); err != nil {
		logger.Errorf(ctx, "Failed to write protobuf (metadata). Error [%v]", err)
		return executors.NodeStatusUndefined, errors.Wrapf(errors.CausedByError, startNode.GetID(), err, "Failed to store workflow inputs (as start node)")
	}
	return executors.NodeStatusComplete, nil
}

func (c *nodeExecutor) RecursiveNodeHandler(ctx context.Context, w v1alpha1.ExecutableWorkflow, currentNode v1alpha1.ExecutableNode) (executors.NodeStatus, error) {
	currentNodeCtx := contextutils.WithNodeID(ctx, currentNode.GetID())
	nodeStatus := w.GetNodeExecutionStatus(currentNode.GetID())
	switch nodeStatus.GetPhase() {
	case v1alpha1.NodePhaseNotYetStarted, v1alpha1.NodePhaseQueued, v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseTimingOut, v1alpha1.NodePhaseRetryableFailure, v1alpha1.NodePhaseSucceeding:
		logger.Debugf(currentNodeCtx, "Handling node Status [%v]", nodeStatus.GetPhase().String())

		t := c.metrics.NodeExecutionTime.Start(ctx)
		defer t.Stop()
		return c.handleNode(currentNodeCtx, w, currentNode)

		// TODO we can optimize skip state handling by iterating down the graph and marking all as skipped
		// Currently we treat either Skip or Success the same way. In this approach only one node will be skipped
		// at a time. As we iterate down, further nodes will be skipped
	case v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseSkipped:
		return c.handleDownstream(ctx, w, currentNode)
	case v1alpha1.NodePhaseFailed:
		logger.Debugf(currentNodeCtx, "Node Failed")
		return executors.NodeStatusFailed(errors.Errorf(errors.RuntimeExecutionError, currentNode.GetID(), "Node Failed.")), nil
	case v1alpha1.NodePhaseTimedOut:
		logger.Debugf(currentNodeCtx, "Node Timed Out")
		return executors.NodeStatusTimedOut, nil
	}
	return executors.NodeStatusUndefined, errors.Errorf(errors.IllegalStateError, currentNode.GetID(), "Should never reach here")
}

func (c *nodeExecutor) FinalizeHandler(ctx context.Context, w v1alpha1.ExecutableWorkflow, currentNode v1alpha1.ExecutableNode) error {
	nodeStatus := w.GetNodeExecutionStatus(currentNode.GetID())
	switch nodeStatus.GetPhase() {
	case v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseSucceeding, v1alpha1.NodePhaseRetryableFailure:
		ctx = contextutils.WithNodeID(ctx, currentNode.GetID())
		nodeStatus := w.GetNodeExecutionStatus(currentNode.GetID())

		// Now depending on the node type decide
		h, err := c.nodeHandlerFactory.GetHandler(currentNode.GetKind())
		if err != nil {
			return err
		}

		nCtx, err := c.newNodeExecContextDefault(ctx, w, currentNode, nodeStatus)
		if err != nil {
			return err
		}
		// Abort this node
		err = c.finalize(ctx, h, nCtx)
		if err != nil {
			return err
		}
	default:
		// Abort downstream nodes
		downstreamNodes, err := w.FromNode(currentNode.GetID())
		if err != nil {
			logger.Debugf(ctx, "Error when retrieving downstream nodes. Error [%v]", err)
			return nil
		}

		errs := make([]error, 0, len(downstreamNodes))
		for _, d := range downstreamNodes {
			downstreamNode, ok := w.GetNode(d)
			if !ok {
				return errors.Errorf(errors.BadSpecificationError, currentNode.GetID(), "Unable to find Downstream Node [%v]", d)
			}

			if err := c.FinalizeHandler(ctx, w, downstreamNode); err != nil {
				logger.Infof(ctx, "Failed to abort node [%v]. Error: %v", d, err)
				errs = append(errs, err)
			}
		}

		if len(errs) > 0 {
			return errors.ErrorCollection{Errors: errs}
		}

		return nil
	}

	return nil
}

func (c *nodeExecutor) AbortHandler(ctx context.Context, w v1alpha1.ExecutableWorkflow, currentNode v1alpha1.ExecutableNode, reason string) error {
	nodeStatus := w.GetNodeExecutionStatus(currentNode.GetID())
	switch nodeStatus.GetPhase() {
	case v1alpha1.NodePhaseRunning, v1alpha1.NodePhaseFailing, v1alpha1.NodePhaseSucceeding, v1alpha1.NodePhaseRetryableFailure, v1alpha1.NodePhaseQueued:
		ctx = contextutils.WithNodeID(ctx, currentNode.GetID())
		nodeStatus := w.GetNodeExecutionStatus(currentNode.GetID())

		// Now depending on the node type decide
		h, err := c.nodeHandlerFactory.GetHandler(currentNode.GetKind())
		if err != nil {
			return err
		}

		nCtx, err := c.newNodeExecContextDefault(ctx, w, currentNode, nodeStatus)
		if err != nil {
			return err
		}
		// Abort this node
		err = c.abort(ctx, h, nCtx, reason)
		if err != nil {
			return err
		}
		nodeExecID := &core.NodeExecutionIdentifier{
			NodeId:      nCtx.NodeID(),
			ExecutionId: w.GetExecutionID().WorkflowExecutionIdentifier,
		}
		err = c.IdempotentRecordEvent(ctx, &event.NodeExecutionEvent{
			Id:         nodeExecID,
			Phase:      core.NodeExecution_ABORTED,
			OccurredAt: ptypes.TimestampNow(),
			OutputResult: &event.NodeExecutionEvent_Error{
				Error: &core.ExecutionError{
					Code:    "NodeAborted",
					Message: reason,
				},
			},
		})
		if err != nil {
			if errors2.IsCausedBy(err, errors.IllegalStateError) {
				logger.Debugf(ctx, "Failed to record abort event due to illegal state transition. Ignoring the error. Error: %v", err)
			} else {
				logger.Warningf(ctx, "Failed to record nodeEvent, error [%s]", err.Error())
				return errors.Wrapf(errors.EventRecordingFailed, nCtx.NodeID(), err, "failed to record node event")
			}
		}
	case v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseSkipped:
		// Abort downstream nodes
		downstreamNodes, err := w.FromNode(currentNode.GetID())
		if err != nil {
			logger.Debugf(ctx, "Error when retrieving downstream nodes. Error [%v]", err)
			return nil
		}

		errs := make([]error, 0, len(downstreamNodes))
		for _, d := range downstreamNodes {
			downstreamNode, ok := w.GetNode(d)
			if !ok {
				return errors.Errorf(errors.BadSpecificationError, currentNode.GetID(), "Unable to find Downstream Node [%v]", d)
			}

			if err := c.AbortHandler(ctx, w, downstreamNode, reason); err != nil {
				logger.Infof(ctx, "Failed to abort node [%v]. Error: %v", d, err)
				errs = append(errs, err)
			}
		}

		if len(errs) > 0 {
			return errors.ErrorCollection{Errors: errs}
		}

		return nil
	default:
		ctx = contextutils.WithNodeID(ctx, currentNode.GetID())
		logger.Warnf(ctx, "Trying to abort a node in state [%s]", nodeStatus.GetPhase().String())
	}
	return nil
}

func (c *nodeExecutor) Initialize(ctx context.Context) error {
	logger.Infof(ctx, "Initializing Core Node Executor")
	s := c.newSetupContext(ctx)
	return c.nodeHandlerFactory.Setup(ctx, s)
}

func NewExecutor(ctx context.Context, defaultDeadlines config.DefaultDeadlines, store *storage.DataStore, enQWorkflow v1alpha1.EnqueueWorkflow, eventSink events.EventSink, workflowLauncher launchplan.Executor, maxDatasetSize int64, kubeClient executors.Client, catalogClient catalog.Client, scope promutils.Scope) (executors.Node, error) {

	nodeScope := scope.NewSubScope("node")
	exec := &nodeExecutor{
		store:               store,
		enqueueWorkflow:     enQWorkflow,
		nodeRecorder:        events.NewNodeEventRecorder(eventSink, nodeScope),
		taskRecorder:        events.NewTaskEventRecorder(eventSink, scope.NewSubScope("task")),
		maxDatasetSizeBytes: maxDatasetSize,
		metrics: &nodeMetrics{
			Scope:                  nodeScope,
			FailureDuration:        labeled.NewStopWatch("failure_duration", "Indicates the total execution time of a failed workflow.", time.Millisecond, nodeScope, labeled.EmitUnlabeledMetric),
			SuccessDuration:        labeled.NewStopWatch("success_duration", "Indicates the total execution time of a successful workflow.", time.Millisecond, nodeScope, labeled.EmitUnlabeledMetric),
			InputsWriteFailure:     labeled.NewCounter("inputs_write_fail", "Indicates failure in writing node inputs to metastore", nodeScope),
			TimedOutFailure:        labeled.NewCounter("timeout_fail", "Indicates failure due to timeout", nodeScope),
			ResolutionFailure:      labeled.NewCounter("input_resolve_fail", "Indicates failure in resolving node inputs", nodeScope),
			TransitionLatency:      labeled.NewStopWatch("transition_latency", "Measures the latency between the last parent node stoppedAt time and current node's queued time.", time.Millisecond, nodeScope, labeled.EmitUnlabeledMetric),
			QueuingLatency:         labeled.NewStopWatch("queueing_latency", "Measures the latency between the time a node's been queued to the time the handler reported the executable moved to running state", time.Millisecond, nodeScope, labeled.EmitUnlabeledMetric),
			NodeExecutionTime:      labeled.NewStopWatch("node_exec_latency", "Measures the time taken to execute one node, a node can be complex so it may encompass sub-node latency.", time.Microsecond, nodeScope, labeled.EmitUnlabeledMetric),
			NodeInputGatherLatency: labeled.NewStopWatch("node_input_latency", "Measures the latency to aggregate inputs and check readiness of a node", time.Millisecond, nodeScope, labeled.EmitUnlabeledMetric),
		},
		outputResolver:           NewRemoteFileOutputResolver(store),
		defaultExecutionDeadline: defaultDeadlines.DefaultNodeExecutionDeadline.Duration,
		defaultActiveDeadline:    defaultDeadlines.DefaultNodeActiveDeadline.Duration,
	}
	nodeHandlerFactory, err := NewHandlerFactory(ctx, exec, workflowLauncher, kubeClient, catalogClient, nodeScope)
	exec.nodeHandlerFactory = nodeHandlerFactory
	return exec, err
}

func init() {
	labeled.SetMetricKeys(contextutils.ProjectKey, contextutils.DomainKey, contextutils.WorkflowIDKey,
		contextutils.TaskIDKey)
}
