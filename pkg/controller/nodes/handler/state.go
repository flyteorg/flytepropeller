package handler

import (
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

// This is the legacy state structure that gets translated to node status
// TODO eventually we could just convert this to be binary node state encoded into the node status

type TaskNodeState struct {
	PluginPhase        pluginCore.Phase
	PluginPhaseVersion uint32
	PluginState        []byte
	PluginStateVersion uint32
	BarrierClockTick   uint32
}

type BranchNodeState struct {
	FinalizedNodeID *v1alpha1.NodeID
	Phase           v1alpha1.BranchNodePhase
}

type DynamicNodePhase uint8

type DynamicNodeState struct {
	Phase  v1alpha1.DynamicNodePhase
	Reason string
}

type WorkflowNodeState struct {
	Phase v1alpha1.WorkflowNodePhase
}

type NodeStateWriter interface {
	PutTaskNodeState(s TaskNodeState) error
	PutBranchNode(s BranchNodeState) error
	PutDynamicNodeState(s DynamicNodeState) error
	PutWorkflowNodeState(s WorkflowNodeState) error
}

type NodeStateReader interface {
	GetTaskNodeState() TaskNodeState
	GetBranchNode() BranchNodeState
	GetDynamicNodeState() DynamicNodeState
	GetWorkflowNodeState() WorkflowNodeState
}
