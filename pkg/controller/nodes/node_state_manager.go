package nodes

import (
	"context"

	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

type legacyTaskPhase = int

const (
	legacyTaskPhaseQueued legacyTaskPhase = iota
	legacyTaskPhaseRunning
	legacyTaskPhaseRetryableFailure
	legacyTaskPhasePermanentFailure
	legacyTaskPhaseSucceeded
	legacyTaskPhaseUndefined
	legacyTaskPhaseNotReady
	legacyTaskPhaseUnknown
)

func legacyPluginPhaseToNew(p int) pluginCore.Phase {
	switch p {
	case legacyTaskPhaseQueued:
		return pluginCore.PhaseQueued
	case legacyTaskPhaseRunning:
		return pluginCore.PhaseRunning
	case legacyTaskPhaseRetryableFailure:
		return pluginCore.PhaseRetryableFailure
	case legacyTaskPhasePermanentFailure:
		return pluginCore.PhasePermanentFailure
	case legacyTaskPhaseSucceeded:
		return pluginCore.PhaseSuccess
	case legacyTaskPhaseUndefined:
		return pluginCore.PhaseUndefined
	case legacyTaskPhaseNotReady:
		return pluginCore.PhaseNotReady
	case legacyTaskPhaseUnknown:
		return pluginCore.PhaseUndefined
	}
	return pluginCore.PhaseUndefined
}

type nodeStateManager struct {
	nodeStatus v1alpha1.ExecutableNodeStatus
	t          *handler.TaskNodeState
	b          *handler.BranchNodeState
	d          *handler.DynamicNodeState
}

func (n nodeStateManager) PutTaskNodeState(s handler.TaskNodeState) error {
	n.t = &s
	return nil
}

func (n nodeStateManager) PutBranchNode(s handler.BranchNodeState) error {
	n.b = &s
	return nil
}

func (n nodeStateManager) PutDynamicNodeState(s handler.DynamicNodeState) error {
	n.d = &s
	return nil
}

func (n nodeStateManager) GetTaskNodeState() handler.TaskNodeState {
	tn := n.nodeStatus.GetTaskNodeStatus()
	if tn != nil {
		var p pluginCore.Phase
		var legacyState map[string]interface{}
		// Compatibility code, delete after migration
		if !tn.UsePluginState() {
			logger.Warnf(context.TODO(), "old task node state retrieved. ")
			p = legacyPluginPhaseToNew(tn.GetPhase())
			legacyState = tn.GetCustomState()
		} else {
			p = pluginCore.Phase(tn.GetPhase())
		}
		return handler.TaskNodeState{
			PluginStateLegacy:  legacyState,
			PluginPhase:        p,
			PluginPhaseVersion: tn.GetPhaseVersion(),
			PluginStateVersion: tn.GetPluginStateVersion(),
			PluginState:        tn.GetPluginState(),
		}
	}
	return handler.TaskNodeState{}
}

func (n nodeStateManager) GetBranchNode() handler.BranchNodeState {
	bn := n.nodeStatus.GetBranchStatus()
	bs := handler.BranchNodeState{}
	if bn != nil {
		bs.Phase = bn.GetPhase()
		bs.FinalizedNodeID = bn.GetFinalizedNode()
	}
	return bs
}

func (n nodeStateManager) GetDynamicNodeState() handler.DynamicNodeState {
	dn := n.nodeStatus.GetDynamicNodeStatus()
	ds := handler.DynamicNodeState{}
	if dn != nil {
		ds.Phase = dn.GetDynamicNodePhase()
	}
	return ds
}

func (n nodeStateManager) clearNodeStatus() {
	n.t = nil
	n.b = nil
	n.d = nil
}

func newNodeStateManager(_ context.Context, status v1alpha1.ExecutableNodeStatus) *nodeStateManager {
	return &nodeStateManager{nodeStatus: status}
}
