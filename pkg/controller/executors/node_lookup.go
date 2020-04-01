package executors

import (
	"context"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type NodeLookup interface {
	GetNode(nodeID v1alpha1.NodeID) (v1alpha1.ExecutableNode, bool)
	GetNodeExecutionStatus(ctx context.Context, id v1alpha1.NodeID) v1alpha1.ExecutableNodeStatus
}

type singleNodeLookup struct {
	n v1alpha1.ExecutableNode
	v1alpha1.NodeStatusGetter
}

func (s singleNodeLookup) GetNode(nodeID v1alpha1.NodeID) (v1alpha1.ExecutableNode, bool) {
	if nodeID != s.n.GetID() {
		return nil, false
	}
	return s.n, true
}

func NewSingleNodeLookup(n v1alpha1.ExecutableNode, s v1alpha1.NodeStatusGetter) NodeLookup {
	return singleNodeLookup{NodeStatusGetter: s, n: n}
}

type contextualNodeLookup struct {
	v1alpha1.NodeGetter
	v1alpha1.NodeStatusGetter
}

func NewNodeLookup(n v1alpha1.NodeGetter, s v1alpha1.NodeStatusGetter) NodeLookup {
	return contextualNodeLookup{
		NodeGetter:       n,
		NodeStatusGetter: s,
	}
}
