package staticobjstore

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/static"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytestdlib/storage"
)

type InmemoryStaticObjStore struct {
	store     map[string]*static.WorkflowStaticExecutionObj
	dataStore *storage.DataStore
}

func (i *InmemoryStaticObjStore) Get(ctx context.Context, wf *v1alpha1.FlyteWorkflow) (*static.WorkflowStaticExecutionObj, error) {

	loc := wf.WorkflowStaticExecutionObj

	if loc == "" {
		return nil, ErrStaticObjLocationEmpty
	}

	if m, ok := i.store[loc.String()]; ok {
		return m, nil
	}

	rawReader, err := i.dataStore.ReadRaw(ctx, loc)
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

	wfStaticExecutionObj := &static.WorkflowStaticExecutionObj{}
	err = json.Unmarshal(buf.Bytes(), wfStaticExecutionObj)
	if err != nil {
		return nil, err
	}

	i.store[loc.String()] = wfStaticExecutionObj

	return wfStaticExecutionObj, nil
}

func (i *InmemoryStaticObjStore) Remove(ctx context.Context, wf *v1alpha1.FlyteWorkflow) error {
	delete(i.store, wf.WorkflowStaticExecutionObj.String())
	return nil
}

func NewInmemoryStaticObjStore(dataStore *storage.DataStore) *InmemoryStaticObjStore {
	return &InmemoryStaticObjStore{
		store:     map[string]*static.WorkflowStaticExecutionObj{},
		dataStore: dataStore,
	}
}
