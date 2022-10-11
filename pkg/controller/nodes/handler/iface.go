package handler

import (
	"context"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/catalog"
)

//go:generate mockery -all -case=underscore

// Interface that should be implemented for a node type.
type Node interface {
	// Method to indicate that finalize is required for this handler
	FinalizeRequired() bool

	// Setup should be called, before invoking any other methods of this handler in a single thread context
	Setup(ctx context.Context, setupContext SetupContext) error

	// Core method that should handle this node
	Handle(ctx context.Context, executionContext NodeExecutionContext) (Transition, error)

	// This method should be invoked to indicate the node needs to be aborted.
	Abort(ctx context.Context, executionContext NodeExecutionContext, reason string) error

	// This method is always called before completing the node, if FinalizeRequired returns true.
	// It is guaranteed that Handle -> (happens before) -> Finalize. Abort -> finalize may be repeated multiple times
	Finalize(ctx context.Context, executionContext NodeExecutionContext) error
}

// CacheableNode is a node that supports caching
type CacheableNode interface {
	Node

	// GetCatalogKey returns the unique key for the node represented by the NodeExecutionContext
	GetCatalogKey(ctx context.Context, executionContext NodeExecutionContext) (catalog.Key, error)

	// IsCacheable returns two booleans representing if the node represented by the
	// NodeExecutionContext is cacheable and cache serializable respectively.
	IsCacheable(ctx context.Context, executionContext NodeExecutionContext) (bool, bool, error)
}
