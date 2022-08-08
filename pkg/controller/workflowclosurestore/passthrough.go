package workflowclosurestore

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytestdlib/storage"
)

type passthroughWorkflowClosureStore struct {
	dataStore *storage.DataStore
}

func (p *passthroughWorkflowClosureStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*core.CompiledWorkflowClosure, error) {
	if len(dataReference) == 0 {
		return nil, ErrLocationEmpty
	}

	workflowClosure := &core.CompiledWorkflowClosure{}
	err := p.dataStore.ReadProtobuf(ctx, dataReference, workflowClosure)
	if err != nil {
		return nil, err
	}

	return workflowClosure, nil
}

func (p *passthroughWorkflowClosureStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	return nil
}

func NewPassthroughWorkflowClosureStore(dataStore *storage.DataStore) WorkflowClosureStore {
	return &passthroughWorkflowClosureStore{
		dataStore: dataStore,
	}
}
