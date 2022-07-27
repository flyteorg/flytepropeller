package crdoffloadstore

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"

	"github.com/flyteorg/flytestdlib/storage"
)

type passthroughCRDOffloadStore struct {
	dataStore *storage.DataStore
}

func (p *passthroughCRDOffloadStore) Get(ctx context.Context, dataReference v1alpha1.DataReference) (*v1alpha1.StaticWorkflowData, error) {
	if len(dataReference) == 0 {
		return nil, ErrLocationEmpty
	}

	rawReader, err := p.dataStore.ReadRaw(ctx, dataReference)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	_, err = buf.ReadFrom(rawReader)
	if err != nil {
		return nil, err
	}

	err = rawReader.Close()
	if err != nil {
		return nil, err
	}

	staticWorkflowData := &v1alpha1.StaticWorkflowData{}
	err = json.Unmarshal(buf.Bytes(), staticWorkflowData)
	if err != nil {
		return nil, err
	}

	return staticWorkflowData, nil
}

func (p *passthroughCRDOffloadStore) Remove(ctx context.Context, dataReference v1alpha1.DataReference) error {
	return nil
}

func NewPassthroughCRDOffloadStore(dataStore *storage.DataStore) CRDOffloadStore {
	return &passthroughCRDOffloadStore{
		dataStore: dataStore,
	}
}

