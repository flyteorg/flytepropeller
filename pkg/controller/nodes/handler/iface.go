package handler

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
	"github.com/flyteorg/flytestdlib/promutils"
)

//go:generate mockery -all -case=underscore

// TODO @hamersaw - docs?!?1
type NodeExecutor interface {
	// TODO @hamersaw - BuildNodeExecutionContext should be here - removes need for another interface
	HandleNode(ctx context.Context, dag executors.DAGStructure, nCtx interfaces.NodeExecutionContext, h Node) (interfaces.NodeStatus, error)
	Abort(ctx context.Context, h Node, nCtx interfaces.NodeExecutionContext, reason string) error
	Finalize(ctx context.Context, h Node, nCtx interfaces.NodeExecutionContext) error
}

// Interface that should be implemented for a node type.
type Node interface {
	// Method to indicate that finalize is required for this handler
	FinalizeRequired() bool

	// Setup should be called, before invoking any other methods of this handler in a single thread context
	Setup(ctx context.Context, setupContext SetupContext) error

	// Core method that should handle this node
	Handle(ctx context.Context, executionContext interfaces.NodeExecutionContext) (Transition, error)

	// This method should be invoked to indicate the node needs to be aborted.
	Abort(ctx context.Context, executionContext interfaces.NodeExecutionContext, reason string) error

	// This method is always called before completing the node, if FinalizeRequired returns true.
	// It is guaranteed that Handle -> (happens before) -> Finalize. Abort -> finalize may be repeated multiple times
	Finalize(ctx context.Context, executionContext interfaces.NodeExecutionContext) error
}

type SetupContext interface {
	EnqueueOwner() func(string)
	OwnerKind() string
	MetricsScope() promutils.Scope
}
