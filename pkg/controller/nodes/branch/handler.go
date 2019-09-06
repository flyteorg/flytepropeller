package branch

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

type branchHandler struct {
	nodeExecutor executors.Node
}

func (b *branchHandler) Setup(ctx context.Context, setupContext handler.SetupContext) error {
	panic("implement me")
}

func (b *branchHandler) Handle(ctx context.Context, executionContext handler.NodeExecutionContext) (handler.Transition, error) {
	panic("implement me")
}

func (b *branchHandler) Abort(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	panic("implement me")
}

func (b *branchHandler) Finalize(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	panic("implement me")
}

func New(executor executors.Node, scope promutils.Scope) handler.Node {
	//branchScope := scope.NewSubScope("branch")
	return &branchHandler{
		nodeExecutor: executor,
	}
}
