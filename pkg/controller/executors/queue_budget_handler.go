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

	nodeBudget := in.wfBudget
	for _, upstreamNodeID := range upstreamNodes {
		upstreamNodeStatus := in.nl.GetNodeExecutionStatus(ctx, upstreamNodeID)

		if upstreamNodeStatus.GetPhase() == v1alpha1.NodePhaseSkipped {
			// TODO handle skipped parent case: if parent doesn't have queue budget info then get it from its parent.
			continue
		}

		var budget time.Duration
		if upstreamNodeStatus.GetQueuingBudget() != nil && *&upstreamNodeStatus.GetQueuingBudget().Duration > 0 {
			budget = *&upstreamNodeStatus.GetQueuingBudget().Duration
		}

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

	currNode, exists := in.nl.GetNode(id)
	if !exists {
		// mtoledo: what should be the error here
		return nil, err
	}
	var interruptible bool
	if currNode.IsInterruptible() != nil {
		interruptible = *currNode.IsInterruptible()
	}
	// TODO: Where to get config value from?
	//// a node is not considered interruptible if the system failures have exceeded the configured threshold
	// currNodeStatus := in.nl.GetNodeExecutionStatus(ctx, id)
	//if interruptible && currNodeStatus.GetSystemFailures() >= c.interruptibleFailureThreshold {
	//	interruptible = false
	//	c.metrics.InterruptedThresholdHit.Inc(ctx)
	//}
	//

	return &NodeQueuingParameters{IsInterruptible: interruptible, MaxQueueTime: time.Second * time.Duration(nodeBudget)}, nil
}

// instead of *int64 use duration?
func NewDefaultQueuingBudgetHandler(dag DAGStructure, nl NodeLookup, queueingBudget *int64) QueuingBudgetHandler {
	return &defaultQueuingBudgetHandler{dag: dag, nl: nl, wfBudget: time.Duration(*queueingBudget)}
}
