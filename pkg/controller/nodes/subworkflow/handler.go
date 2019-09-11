package subworkflow

import (
	"context"

	"github.com/lyft/flytestdlib/promutils"

	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
)

type workflowNodeHandler struct {
}

func (w *workflowNodeHandler) FinalizeRequired() bool {
	return true
}

func (w *workflowNodeHandler) Setup(ctx context.Context, setupContext handler.SetupContext) error {
	panic("implement me")
}

func (w *workflowNodeHandler) Handle(ctx context.Context, executionContext handler.NodeExecutionContext) (handler.Transition, error) {
	panic("implement me")
}

func (w *workflowNodeHandler) Abort(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	panic("implement me")
}

func (w *workflowNodeHandler) Finalize(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	panic("implement me")
}

func New(executor executors.Node, workflowLauncher launchplan.Executor, scope promutils.Scope) handler.Node {
	return &workflowNodeHandler{}
}
