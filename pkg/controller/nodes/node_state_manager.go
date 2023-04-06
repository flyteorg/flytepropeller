package nodes

import (
	"context"

	pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
)

type nodeStateManager struct {
	nodeStatus v1alpha1.ExecutableNodeStatus
	t          *interfaces.TaskNodeState
	b          *interfaces.BranchNodeState
	d          *interfaces.DynamicNodeState
	w          *interfaces.WorkflowNodeState
	g          *interfaces.GateNodeState
	a          *interfaces.ArrayNodeState
}

func (n *nodeStateManager) PutTaskNodeState(s interfaces.TaskNodeState) error {
	n.t = &s
	return nil
}

func (n *nodeStateManager) PutBranchNode(s interfaces.BranchNodeState) error {
	n.b = &s
	return nil
}

func (n *nodeStateManager) PutDynamicNodeState(s interfaces.DynamicNodeState) error {
	n.d = &s
	return nil
}

func (n *nodeStateManager) PutWorkflowNodeState(s interfaces.WorkflowNodeState) error {
	n.w = &s
	return nil
}

func (n *nodeStateManager) PutGateNodeState(s interfaces.GateNodeState) error {
	n.g = &s
	return nil
}

func (n *nodeStateManager) PutArrayNodeState(s interfaces.ArrayNodeState) error {
	n.a = &s
	return nil
}

func (n nodeStateManager) GetTaskNodeState() interfaces.TaskNodeState {
	tn := n.nodeStatus.GetTaskNodeStatus()
	if tn != nil {
		return interfaces.TaskNodeState{
			PluginPhase:                        pluginCore.Phase(tn.GetPhase()),
			PluginPhaseVersion:                 tn.GetPhaseVersion(),
			PluginStateVersion:                 tn.GetPluginStateVersion(),
			PluginState:                        tn.GetPluginState(),
			BarrierClockTick:                   tn.GetBarrierClockTick(),
			LastPhaseUpdatedAt:                 tn.GetLastPhaseUpdatedAt(),
			PreviousNodeExecutionCheckpointURI: tn.GetPreviousNodeExecutionCheckpointPath(),
		}
	}
	return interfaces.TaskNodeState{}
}

func (n nodeStateManager) GetBranchNode() interfaces.BranchNodeState {
	bn := n.nodeStatus.GetBranchStatus()
	bs := interfaces.BranchNodeState{}
	if bn != nil {
		bs.Phase = bn.GetPhase()
		bs.FinalizedNodeID = bn.GetFinalizedNode()
	}
	return bs
}

func (n nodeStateManager) GetDynamicNodeState() interfaces.DynamicNodeState {
	dn := n.nodeStatus.GetDynamicNodeStatus()
	ds := interfaces.DynamicNodeState{}
	if dn != nil {
		ds.Phase = dn.GetDynamicNodePhase()
		ds.Reason = dn.GetDynamicNodeReason()
		ds.Error = dn.GetExecutionError()
	}

	return ds
}

func (n nodeStateManager) GetWorkflowNodeState() interfaces.WorkflowNodeState {
	wn := n.nodeStatus.GetWorkflowNodeStatus()
	ws := interfaces.WorkflowNodeState{}
	if wn != nil {
		ws.Phase = wn.GetWorkflowNodePhase()
		ws.Error = wn.GetExecutionError()
	}
	return ws
}

func (n nodeStateManager) GetGateNodeState() interfaces.GateNodeState {
	gn := n.nodeStatus.GetGateNodeStatus()
	gs := interfaces.GateNodeState{}
	if gn != nil {
		gs.Phase = gn.GetGateNodePhase()
	}
	return gs
}

func (n nodeStateManager) GetArrayNodeState() interfaces.ArrayNodeState {
	an := n.nodeStatus.GetArrayNodeStatus()
	as := interfaces.ArrayNodeState{}
	if an != nil {
		as.Phase = an.GetArrayNodePhase()
		as.SubNodePhases = an.GetSubNodePhases()
	}
	return as
}

func (n *nodeStateManager) clearNodeStatus() {
	n.t = nil
	n.b = nil
	n.d = nil
	n.w = nil
	n.g = nil
	n.a = nil
	n.nodeStatus.ClearLastAttemptStartedAt()
}

func newNodeStateManager(_ context.Context, status v1alpha1.ExecutableNodeStatus) *nodeStateManager {
	return &nodeStateManager{nodeStatus: status}
}
