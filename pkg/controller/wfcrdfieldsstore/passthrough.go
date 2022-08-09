package wfcrdfieldsstore

import (
	"context"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	k8s "github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytestdlib/storage"
)

type passthroughWorkflowClosureStore struct {
	dataStore *storage.DataStore
}

func (p *passthroughWorkflowClosureStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*k8s.WfClosureCrdFields, error) {
	if len(dataReference) == 0 {
		return nil, ErrLocationEmpty
	}

	wfClosure := &core.CompiledWorkflowClosure{}
	err := p.dataStore.ReadProtobuf(ctx, dataReference, wfClosure)
	if err != nil {
		return nil, err
	}

	wfClosureCrdFields, err := k8s.BuildWfClosureCrdFields(wfClosure)
	if err != nil {
		return nil, err
	}

	return wfClosureCrdFields, nil
}

func (p *passthroughWorkflowClosureStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	return nil
}

func NewPassthroughWfClosureCrdFieldsStore(dataStore *storage.DataStore) WfClosureCrdFieldsStore {
	return &passthroughWorkflowClosureStore{
		dataStore: dataStore,
	}
}
