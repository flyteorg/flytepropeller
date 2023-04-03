package array

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"

	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
)

//go:generate mockery -all -case=underscore

// arrayNodeHandler is a handle implementation for processing array nodes
type arrayNodeHandler struct {
	metrics      metrics
}

// metrics encapsulates the prometheus metrics for this handler
type metrics struct {
	scope promutils.Scope
}

// newMetrics initializes a new metrics struct
func newMetrics(scope promutils.Scope) metrics {
	return metrics{
		scope: scope,
	}
}

// Abort stops the array node defined in the NodeExecutionContext
func (a *arrayNodeHandler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext, reason string) error {
	return nil // TODO @hamersaw - implement abort
}

// Finalize completes the array node defined in the NodeExecutionContext
func (a *arrayNodeHandler) Finalize(ctx context.Context, _ handler.NodeExecutionContext) error {
	return nil // TODO @hamersaw - implement finalize
}

// FinalizeRequired defines whether or not this handler requires finalize to be called on
// node completion
func (a *arrayNodeHandler) FinalizeRequired() bool {
	return false // TODO @hamersaw - implement finalize required
}

// Handle is responsible for transitioning and reporting node state to complete the node defined
// by the NodeExecutionContext
func (a *arrayNodeHandler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {
	arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

	// TODO @hamersaw - handle array node

	// update array node status
	if err := nCtx.NodeStateWriter().PutArrayNodeState(arrayNodeState); err != nil {
		logger.Errorf(ctx, "failed to store ArrayNode state with err [%s]", err.Error())
		return handler.UnknownTransition, err
	}

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})), nil
}

// Setup handles any initialization requirements for this handler
func (a *arrayNodeHandler) Setup(_ context.Context, _ handler.SetupContext) error {
	return nil // TODO @hamersaw - implement setup
}

// New initializes a new arrayNodeHandler
func New(eventConfig *config.EventConfig, scope promutils.Scope) handler.Node {
	arrayScope := scope.NewSubScope("array")
	return &arrayNodeHandler{
		metrics:      newMetrics(arrayScope),
	}
}
