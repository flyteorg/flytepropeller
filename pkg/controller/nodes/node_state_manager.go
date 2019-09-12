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
	TaskPhaseQueued legacyTaskPhase = iota
	TaskPhaseRunning
	TaskPhaseRetryableFailure
	TaskPhasePermanentFailure
	TaskPhaseSucceeded
	TaskPhaseUndefined
	TaskPhaseNotReady
	TaskPhaseUnknown
)

func legacyPluginPhaseToNew(p int) pluginCore.Phase {
	switch p {
	case TaskPhaseQueued:
		return pluginCore.PhaseQueued
	case TaskPhaseRunning:
		return pluginCore.PhaseRunning
	case TaskPhaseRetryableFailure:
		return pluginCore.PhaseRetryableFailure
	case TaskPhasePermanentFailure:
		return pluginCore.PhasePermanentFailure
	case TaskPhaseSucceeded:
		return pluginCore.PhaseSuccess
	case TaskPhaseUndefined:
		return pluginCore.PhaseUndefined
	case TaskPhaseNotReady:
		return pluginCore.PhaseNotReady
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
	bn := n.nodeStatus.GetOrCreateBranchStatus()
	// TODO maybe we should have a get and check if it is nil, as we can now easily create it!
	panic("implement me")
}

func (n nodeStateManager) GetDynamicNodeState() handler.DynamicNodeState {
	// TODO maybe we should have a get and check if it is nil, as we can now easily create it!
	panic("implement me")
}

func (n nodeStateManager) clearNodeStatus() {
	n.t = nil
	n.b = nil
	n.d = nil
}

func newNodeStateManager(_ context.Context, status v1alpha1.ExecutableNodeStatus) *nodeStateManager {
	return &nodeStateManager{nodeStatus: status}
}
