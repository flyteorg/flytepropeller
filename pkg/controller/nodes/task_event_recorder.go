package nodes

import (
	"context"

	"github.com/lyft/flyteidl/clients/go/events"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/lyft/flytestdlib/logger"

	eventsErr "github.com/lyft/flyteidl/clients/go/events/errors"
)

type taskEventRecorder struct {
	events.TaskEventRecorder
}

func (t taskEventRecorder) RecordTaskEvent(ctx context.Context, ev *event.TaskExecutionEvent) error {
	if err := t.TaskEventRecorder.RecordTaskEvent(ctx, ev); err != nil {
		if eventsErr.IsEventAlreadyInTerminalStateError(err) || eventsErr.IsAlreadyExists(err) {
			logger.Warningf(ctx, "Failed to record taskEvent, error [%s]. Ignoring this error!", err.Error())
		} else {
			return err
		}
	}
	return nil
}
