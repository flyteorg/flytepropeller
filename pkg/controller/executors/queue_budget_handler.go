package executors

import (
	"context"
	"time"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

// Interface for the Workflow p. This is the mutable portion for a Workflow
type QueuingBudgetHandler interface {
	GetNodeQueuingParameters(ctx context.Context, id v1alpha1.NodeID) (*NodeQueuingParameters, error)
}

type NodeQueuingParameters struct {
	IsInterruptible bool
	MaxQueueTime    time.Duration
}

type defaultQueuingBudgetHandler struct {
	dag      DAGStructure
	nl       NodeLookup
	wfBudget time.Duration
}

func (in *defaultQueuingBudgetHandler) GetNodeQueuingParameters(ctx context.Context, id v1alpha1.NodeID) (*NodeQueuingParameters, error) {

	if id == v1alpha1.StartNodeID {
		return nil, nil
	}

	upstreamNodes, err := in.dag.ToNode(id)
	if err != nil {
		return nil, err
	}

	// TODO init with wf budget or default value
	nodeBudget := in.wfBudget
	for _, upstreamNodeID := range upstreamNodes {
		upstreamNodeStatus := in.nl.GetNodeExecutionStatus(ctx, upstreamNodeID)

		if upstreamNodeStatus.GetPhase() == v1alpha1.NodePhaseSkipped {
			// TODO handle skipped parent case: if parent doesn't have queue budget info then get it from its parent.
			continue
		}

		budget := time.Second // TODO assign

		// fix this
		//if upstreamNodeStatus.GetMaxQueueTimeSeconds() != nil && *upstreamNodeStatus.GetMaxQueueTimeSeconds() > 0 {
		//	budget = *upstreamNodeStatus.GetMaxQueueTimeSeconds()
		//}

		if upstreamNodeStatus.GetQueuedAt() != nil {
			queuedAt := upstreamNodeStatus.GetQueuedAt().Time
			if upstreamNodeStatus.GetLastAttemptStartedAt() == nil {
				// nothing used
			}
			lastAttemptStartedAt := upstreamNodeStatus.GetLastAttemptStartedAt().Time
			queuingDelay := lastAttemptStartedAt.Sub(queuedAt)
			parentRemainingBudget := budget - queuingDelay

			if nodeBudget > parentRemainingBudget {
				nodeBudget = parentRemainingBudget
			}
		}
	}

	// TODO: fix this
	//interruptible := executionContext.IsInterruptible()
	//if n.IsInterruptible() != nil {
	//	interruptible = *n.IsInterruptible()
	//}
	//
	//s := nl.GetNodeExecutionStatus(ctx, currentNodeID)
	//
	//// a node is not considered interruptible if the system failures have exceeded the configured threshold
	//if interruptible && s.GetSystemFailures() >= c.interruptibleFailureThreshold {
	//	interruptible = false
	//	c.metrics.InterruptedThresholdHit.Inc(ctx)
	//}
	//
	isInterruptible := false

	return &NodeQueuingParameters{IsInterruptible: isInterruptible, MaxQueueTime: time.Second * time.Duration(nodeBudget)}, nil
}

func NewDefaultQueuingBudgetHandler(dag DAGStructure, nl NodeLookup, queueingBudget *int64) QueuingBudgetHandler {
	return &defaultQueuingBudgetHandler{dag: dag, nl: nl, wfBudget: time.Duration(*queueingBudget)}
}
