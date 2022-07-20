package k8s

import (
	"context"
	"fmt"
	"github.com/flyteorg/flyteplugins/go/tasks/errors"
	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/backoff"
	"github.com/flyteorg/flytestdlib/logger"
)

// ResourcePluginManager A generic Plugin for managing k8s-resources. Plugin writers wishing to use K8s resource can use the
//simplified api specified in pluginmachinery.core
type ResourcePluginManager struct {
	*PluginManager
}

func (e ResourcePluginManager) Handle(ctx context.Context, tCtx pluginsCore.TaskExecutionContext) (pluginsCore.Transition, error) {
	ps := PluginState{}
	if v, err := tCtx.ResourcePluginStateReader().Get(&ps); err != nil {
		if v != PluginStateVersion {
			return pluginsCore.DoTransition(pluginsCore.PhaseInfoRetryableFailure(errors.CorruptedPluginState, fmt.Sprintf("plugin state version mismatch expected [%d] got [%d]", PluginStateVersion, v), nil)), nil
		}
		return pluginsCore.UnknownTransition, errors.Wrapf(errors.CorruptedPluginState, err, "Failed to read unmarshal custom state")
	}
	logger.Warnf(ctx, "PluginManager PluginState [%v]", ps.Phase)
	if ps.Phase == PluginPhaseNotStarted {
		t, err := e.LaunchResource(ctx, tCtx)
		if err == nil && t.Info().Phase() == pluginsCore.PhaseQueued {
			if err := tCtx.ResourcePluginStateWriter().Put(PluginStateVersion, &PluginState{Phase: PluginPhaseStarted}); err != nil {
				logger.Warnf(ctx, "pluginsCore.UnknownTransition [%v]", t.Info().Phase())
				return pluginsCore.UnknownTransition, err
			}
			logger.Warnf(ctx, "tCtx.rpsm.newState.Bytes() [%s].", ps.Phase)
		}
		return t, err
	}

	return e.CheckResourcePhase(ctx, tCtx)
}
func NewResourcePluginManagerWithBackOff(ctx context.Context, iCtx pluginsCore.SetupContext, entry k8s.PluginEntry, backOffController *backoff.Controller,
	monitorIndex *ResourceMonitorIndex) (*ResourcePluginManager, error) {

	mgr, err := NewPluginManager(ctx, iCtx, entry, monitorIndex)
	if err == nil {
		mgr.backOffController = backOffController
	}
	return &ResourcePluginManager{mgr}, err
}
