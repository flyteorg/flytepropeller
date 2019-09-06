package fakeplugins

import (
	"context"
	"fmt"

	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
)

type HandleResponse struct {
	T   pluginCore.Transition
	Err error
}

// This is a test plugin and can be used to play any scenario responses from a plugin, (exceptions: panic)
// The plugin is to be invoked within a single thread (not thread safe) and is very simple in terms of usage
// It does not use any state and does not drive the state machine using that state. It drives the state machine constantly forward
type ReplayerPlugin struct {
	id                        string
	props                     pluginCore.PluginProperties
	orderedOnHandleResponses  []HandleResponse
	nextOnHandleResponseIdx   int
	orderedAbortResponses     []error
	nextOnAbortResponseIdx    int
	orderedFinalizeResponses  []error
	nextOnFinalizeResponseIdx int
}

func (r ReplayerPlugin) GetID() string {
	return r.id
}

func (r ReplayerPlugin) GetProperties() pluginCore.PluginProperties {
	return r.props
}

func (r ReplayerPlugin) Handle(_ context.Context, _ pluginCore.TaskExecutionContext) (pluginCore.Transition, error) {
	defer func() {
		r.nextOnHandleResponseIdx++
	}()
	if r.nextOnHandleResponseIdx > len(r.orderedOnHandleResponses) {
		return pluginCore.UnknownTransition, fmt.Errorf("plugin Handle Invoked [%d] times, expected [%d]", r.nextOnHandleResponseIdx, len(r.orderedOnHandleResponses))
	}
	hr := r.orderedOnHandleResponses[r.nextOnHandleResponseIdx]
	return hr.T, hr.Err
}

func (r ReplayerPlugin) Abort(_ context.Context, _ pluginCore.TaskExecutionContext) error {
	defer func() {
		r.nextOnAbortResponseIdx++
	}()
	if r.nextOnAbortResponseIdx > len(r.orderedAbortResponses) {
		return fmt.Errorf("plugin Abort Invoked [%d] times, expected [%d]", r.nextOnAbortResponseIdx, len(r.orderedAbortResponses))
	}
	return r.orderedAbortResponses[r.nextOnAbortResponseIdx]
}

func (r ReplayerPlugin) Finalize(_ context.Context, _ pluginCore.TaskExecutionContext) error {
	defer func() {
		r.nextOnAbortResponseIdx++
	}()
	if r.nextOnAbortResponseIdx > len(r.orderedAbortResponses) {
		return fmt.Errorf("plugin Finalize Invoked [%d] times, expected [%d]", r.nextOnAbortResponseIdx, len(r.orderedFinalizeResponses))
	}
	return r.orderedFinalizeResponses[r.nextOnFinalizeResponseIdx]
}

func (r ReplayerPlugin) VerifyAllCallsCompleted() error {
	if r.nextOnFinalizeResponseIdx != len(r.orderedFinalizeResponses)+1 {
		return fmt.Errorf("finalize method expected invocations [%d], actual invocations [%d]", len(r.orderedFinalizeResponses), r.nextOnFinalizeResponseIdx)
	}
	if r.nextOnAbortResponseIdx != len(r.orderedAbortResponses)+1 {
		return fmt.Errorf("abort method expected invocations [%d], actual invocations [%d]", len(r.orderedAbortResponses), r.nextOnAbortResponseIdx)
	}
	if r.nextOnHandleResponseIdx != len(r.orderedOnHandleResponses)+1 {
		return fmt.Errorf("handle method expected invocations [%d], actual invocations [%d]", len(r.orderedOnHandleResponses), r.nextOnHandleResponseIdx)
	}
	return nil
}

func NewReplayer(forTaskID string, props pluginCore.PluginProperties, orderedOnHandleResponses []HandleResponse, orderedAbortResponses, orderedFinalizeResponses []error) *ReplayerPlugin {
	return &ReplayerPlugin{
		id:                       fmt.Sprintf("replayer-for-%s", forTaskID),
		props:                    props,
		orderedOnHandleResponses: orderedOnHandleResponses,
		orderedAbortResponses:    orderedAbortResponses,
		orderedFinalizeResponses: orderedFinalizeResponses,
	}
}
