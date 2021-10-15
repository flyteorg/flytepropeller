package events

import (
	"context"
	"fmt"

	admin2 "github.com/flyteorg/flyteidl/clients/go/admin"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/admin"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/event"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/service"
	"github.com/flyteorg/flytepropeller/events/errors"
	"github.com/flyteorg/flytestdlib/fastcheck"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/golang/protobuf/proto"
	"golang.org/x/time/rate"
)

type adminEventSink struct {
	adminClient service.AdminServiceClient
	filter      fastcheck.Filter
	rateLimiter *rate.Limiter
	cfg         *Config
}

// Constructs a new EventSink that sends events to FlyteAdmin through gRPC
func NewAdminEventSink(ctx context.Context, config *Config, adminClient service.AdminServiceClient, filter fastcheck.Filter) (EventSink, error) {
	rateLimiter := rate.NewLimiter(rate.Limit(config.Rate), config.Capacity)

	eventSink := &adminEventSink{
		adminClient: adminClient,
		filter:      filter,
		rateLimiter: rateLimiter,
		cfg:         config,
	}

	logger.Infof(ctx, "Created new AdminEventSink to Admin service")
	return eventSink, nil
}

// Sends events to the FlyteAdmin service through gRPC
func (s *adminEventSink) Sink(ctx context.Context, message proto.Message) error {
	logger.Debugf(ctx, "AdminEventSink received a new event %s", message.String())

	var id []byte
	switch eventMessage := message.(type) {
	case *event.WorkflowExecutionEvent:
		id = idFromMessage("w")
	case *event.NodeExecutionEvent:
		id = idFromMessage("n")
	case *event.TaskExecutionEvent:
		id = idFromMessage("t")
	default:
		return fmt.Errorf("unknown event type [%s]", eventMessage.String())
	}

	// Short-circuit if event has already been sent
	if s.filter.Contains(ctx, id) {
		return nil
	}

	if s.rateLimiter.Allow() {
		switch eventMessage := message.(type) {
		case *event.WorkflowExecutionEvent:
			request := &admin.WorkflowExecutionEventRequest{
				Event: eventMessage,
			}
			_, err := s.adminClient.CreateWorkflowEvent(ctx, request)

			if err != nil {
				return errors.WrapError(err)
			}
		case *event.NodeExecutionEvent:
			request := &admin.NodeExecutionEventRequest{
				Event: eventMessage,
			}
			_, err := s.adminClient.CreateNodeEvent(ctx, request)
			if err != nil {
				return errors.WrapError(err)
			}
		case *event.TaskExecutionEvent:
			request := &admin.TaskExecutionEventRequest{
				Event: eventMessage,
			}
			_, err := s.adminClient.CreateTaskEvent(ctx, request)
			if err != nil {
				return errors.WrapError(err)
			}
		default:
			return fmt.Errorf("unknown event type [%s]", eventMessage.String())
		}
	} else {
		return &errors.EventError{Code: errors.ResourceExhausted,
			Cause: fmt.Errorf("Admin EventSink throttling admin traffic"), Message: "Resource Exhausted"}
	}

	s.filter.Add(ctx, id)
	return nil
}

// Closes the gRPC client connection. This should be deferred on the client does shutdown cleanup.
func (s *adminEventSink) Close() error {
	return nil
}

// TODO hamersaw - fill out
func idFromMessage(op string) []byte {
	return []byte(fmt.Sprintf("%s", op))
}

func initializeAdminClientFromConfig(ctx context.Context) (client service.AdminServiceClient, err error) {
	cfg := admin2.GetConfig(ctx)
	clients, err := admin2.NewClientsetBuilder().WithConfig(cfg).Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize clientset. Error: %w", err)
	}

	return clients.AdminClient(), nil
}

func ConstructEventSink(ctx context.Context, config *Config, scope promutils.Scope) (EventSink, error) {
	switch config.Type {
	case EventSinkLog:
		return NewLogSink()
	case EventSinkFile:
		return NewFileSink(config.FilePath)
	case EventSinkAdmin:
		adminClient, err := initializeAdminClientFromConfig(ctx)
		if err != nil {
			return nil, err
		}

		filter, err := fastcheck.NewOppoBloomFilter(2000, scope.NewSubScope("admin_filter"))
		if err != nil {
			return nil, err
		}

		return NewAdminEventSink(ctx, config, adminClient, filter)
	default:
		return NewStdoutSink()
	}
}
