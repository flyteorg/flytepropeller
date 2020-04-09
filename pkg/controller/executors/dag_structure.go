package executors

import "github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

// An interface that captures the Directed Acyclic Graph structure in which the nodes are connected.
// If NodeLookup and DAGStructure are used together a traversal can be implemented.
type DAGStructure interface {
	// The Starting node for the DAG
	StartNode() v1alpha1.ExecutableNode
	// Lookup for upstream edges, find all node ids from which this node can be reached.
	ToNode(id v1alpha1.NodeID) ([]v1alpha1.NodeID, error)
	// Lookup for downstream edges, find all node ids that can be reached from the given node id.
	FromNode(id v1alpha1.NodeID) ([]v1alpha1.NodeID, error)
}
