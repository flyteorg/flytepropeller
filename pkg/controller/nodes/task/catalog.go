package task

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/pkg/errors"

	"github.com/lyft/flytepropeller/pkg/controller/catalog"
)

type CatalogClient struct {
	// TODO change catalog client to match this structure
	catalog.Client
}

func (c CatalogClient) Get(ctx context.Context, tr pluginCore.TaskReader, input io.InputReader) (*core.LiteralMap, error) {
	tk, err := tr.Read(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read Handler template definition")
	}

	in := input.GetInputPath()
	return c.Client.Get(ctx, tk, in)
}

func (c CatalogClient) Put(ctx context.Context, tCtx pluginCore.TaskExecutionContext, op io.OutputReader) error {
	tk, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to read Handler template definition")
	}

	execID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID()

	return c.Client.Put(ctx, tk, &execID, tCtx.InputReader().GetInputPath(), tCtx.OutputWriter().GetOutputPath())
}

func NewCatalogClient() CatalogClient {
	return CatalogClient{}
}
