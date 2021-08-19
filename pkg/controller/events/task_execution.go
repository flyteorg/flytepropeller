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

// Recorder for Task events
type TaskEventRecorder interface {
	RecordTaskEvent(ctx context.Context, ev *event.TaskExecutionEvent, outputPolicy config.RawOutputPolicy) error
}

type taskEventRecorder struct {
	eventRecorder events.TaskEventRecorder
	store         *storage.DataStore
}

// In certain cases, a successful task execution event can be configured to include raw output data inline. However,
// for large outputs these events may exceed the event recipient's message size limit, so we fallback to passing
// the offloaded output URI instead.
func (c *taskEventRecorder) handleFailure(ctx context.Context, ev *event.TaskExecutionEvent, err error, rawOutputPolicy config.RawOutputPolicy) error {
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
	return c.eventRecorder.RecordTaskEvent(ctx, ev)
}

func (r *taskEventRecorder) RecordTaskEvent(ctx context.Context, ev *event.TaskExecutionEvent, outputPolicy config.RawOutputPolicy) error {
	var origEvent = ev
	if outputPolicy == config.RawOutputPolicyInline && len(ev.GetOutputUri()) > 0 {
		outputs := &core.LiteralMap{}
		err := r.store.ReadProtobuf(ctx, storage.DataReference(ev.GetOutputUri()), outputs)
		if err != nil {
			// Fall back to forwarding along outputs by reference when we can't fetch them.
			outputPolicy = config.RawOutputPolicyReference
		} else {
			origEvent = proto.Clone(ev).(*event.TaskExecutionEvent)
			ev.OutputResult = &event.TaskExecutionEvent_OutputData{
				OutputData: outputs,
			}
		}
	}
	err := r.eventRecorder.RecordTaskEvent(ctx, ev)
	if err != nil {
		return r.handleFailure(ctx, origEvent, err, outputPolicy)
	}
	return nil
}

func NewTaskEventRecorder(eventSink events.EventSink, scope promutils.Scope, store *storage.DataStore) TaskEventRecorder {
	return &taskEventRecorder{
		eventRecorder: events.NewTaskEventRecorder(eventSink, scope),
		store:         store,
	}
}
