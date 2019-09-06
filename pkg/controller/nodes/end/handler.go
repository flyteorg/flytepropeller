package end

import (
	"context"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
)

type endHandler struct {
}

func (e *endHandler) Setup(ctx context.Context, setupContext handler.SetupContext) error {
	return nil
}

func (e *endHandler) Handle(ctx context.Context, executionContext handler.NodeExecutionContext) (handler.Transition, error) {
	inputs, err := executionContext.InputReader().Get(ctx)
	if err != nil {
		return handler.UnknownTransition, err
	}
	if inputs != nil {
		logger.Debugf(ctx, "Workflow has outputs. Storing them.")
		nodeStatus := executionContext.NodeStatus()
		o := v1alpha1.GetOutputsFile(nodeStatus.GetDataDir())
		so := storage.Options{}
		store := executionContext.DataStore()
		if err := store.WriteProtobuf(ctx, o, so, inputs); err != nil {
			logger.Errorf(ctx, "Failed to store workflow outputs. Error [%s]", err)
			return handler.UnknownTransition, errors.Wrapf(errors.CausedByError, executionContext.NodeID(), err, "Failed to store workflow outputs, as end-node")
		}
	}
	logger.Debugf(ctx, "End node success")
	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
}

func (e *endHandler) Abort(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	return nil
}

func (e *endHandler) Finalize(ctx context.Context, executionContext handler.NodeExecutionContext) error {
	return nil
}

func New() handler.Node {
	return &endHandler{
	}
}
