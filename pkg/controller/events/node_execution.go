package events

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/flyteorg/flyteidl/clients/go/events"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/storage"
	"github.com/golang/protobuf/proto"
)

//go:generate mockery -all -output=mocks -case=underscore

// Recorder for Node events
type NodeEventRecorder interface {
	RecordNodeEvent(ctx context.Context, event *event.NodeExecutionEvent, outputPolicy config.RawOutputPolicy) error
}

type nodeEventRecorder struct {
	eventRecorder events.NodeEventRecorder
	store         *storage.DataStore
}

// In certain cases, a successful node execution event can be configured to include raw output data inline. However,
// for large outputs these events may exceed the event recipient's message size limit, so we fallback to passing
// the offloaded output URI instead.
func (r *nodeEventRecorder) handleFailure(ctx context.Context, ev *event.NodeExecutionEvent, err error, rawOutputPolicy config.RawOutputPolicy) error {
	// Only attempt to retry sending an event in the case we tried to send raw output data inline
	if rawOutputPolicy != config.RawOutputPolicyInline || len(ev.GetOutputUri()) > 0 {
		return err
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		// Error was not a status error
		return err
	}
	if !strings.HasPrefix(st.Message(), "message too large") {
		return err
	}

	// This time, we attempt to record the event with the output URI set.
	return r.eventRecorder.RecordNodeEvent(ctx, ev)
}

func (r *nodeEventRecorder) RecordNodeEvent(ctx context.Context, ev *event.NodeExecutionEvent, outputPolicy config.RawOutputPolicy) error {
	var origEvent = ev
	if outputPolicy == config.RawOutputPolicyInline && len(ev.GetOutputUri()) > 0 {
		outputs := &core.LiteralMap{}
		err := r.store.ReadProtobuf(ctx, storage.DataReference(ev.GetOutputUri()), outputs)
		if err != nil {
			// Fall back to forwarding along outputs by reference when we can't fetch them.
			outputPolicy = config.RawOutputPolicyReference
		} else {
			origEvent = proto.Clone(ev).(*event.NodeExecutionEvent)
			ev.OutputResult = &event.NodeExecutionEvent_OutputData{
				OutputData: outputs,
			}
		}
	}
	err := r.eventRecorder.RecordNodeEvent(ctx, ev)
	if err != nil {
		return r.handleFailure(ctx, origEvent, err, outputPolicy)
	}
	return nil
}

func NewNodeEventRecorder(eventSink events.EventSink, scope promutils.Scope, store *storage.DataStore) NodeEventRecorder {
	return &nodeEventRecorder{
		eventRecorder: events.NewNodeEventRecorder(eventSink, scope),
		store:         store,
	}
}
