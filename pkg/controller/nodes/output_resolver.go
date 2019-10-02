package nodes

import (
	"context"
	"reflect"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

type VarName = string

//go:generate mockery -name=OutputResolver -case=underscore

type OutputResolver interface {
	// Extracts a subset of node outputs to literals.
	ExtractOutput(ctx context.Context, w v1alpha1.ExecutableWorkflow, n v1alpha1.ExecutableNode,
		bindToVar VarName) (values *core.Literal, err error)
}

func CreateAliasMap(aliases []v1alpha1.Alias) map[string]string {
	aliasToVarMap := make(map[string]string, len(aliases))
	for _, alias := range aliases {
		aliasToVarMap[alias.GetAlias()] = alias.GetVar()
	}
	return aliasToVarMap
}

// A simple output resolver that expects an outputs.pb at the data directory of the node.
type remoteFileOutputResolver struct {
	store *storage.DataStore
}

func (r remoteFileOutputResolver) ExtractOutput(ctx context.Context, w v1alpha1.ExecutableWorkflow, n v1alpha1.ExecutableNode,
	bindToVar VarName) (values *core.Literal, err error) {
	nodeStatus := w.GetNodeExecutionStatus(n.GetID())
	outputsFileRef := v1alpha1.GetOutputsFile(nodeStatus.GetDataDir())

	index, actualVar, err := ParseVarName(bindToVar)
	if err != nil {
		return nil, err
	}

	aliasMap := CreateAliasMap(n.GetOutputAlias())
	if variable, ok := aliasMap[actualVar]; ok {
		logger.Debugf(ctx, "Mapping [%v].[%v] -> [%v].[%v]", n.GetID(), variable, n.GetID(), bindToVar)
		actualVar = variable
	}

	outputsFileRef = v1alpha1.GetOutputsFile(nodeStatus.GetDataDir())
	if index == nil {
		return resolveSingleOutput(ctx, r.store, n.GetID(), outputsFileRef, actualVar)
	} else {
		return resolveSubtaskOutput(ctx, r.store, n.GetID(), outputsFileRef, *index, actualVar)
	}
}

func resolveSubtaskOutput(ctx context.Context, store storage.ProtobufStore, nodeId string, outputsFileRef storage.DataReference,
	idx int, varName string) (*core.Literal, error) {
	d := &core.LiteralMap{}
	if err := store.ReadProtobuf(ctx, outputsFileRef, d); err != nil {
		return nil, errors.Wrapf(errors.CausedByError, nodeId, err, "Failed to GetPrevious data from dataDir [%v]",
			outputsFileRef)
	}

	if d.Literals == nil {
		return nil, errors.Errorf(errors.OutputsNotFoundError, nodeId,
			"Outputs not found at [%v]", outputsFileRef)
	}

	l, ok := d.Literals[varName]
	if !ok {
		return nil, errors.Errorf(errors.BadSpecificationError, nodeId, "Output of array tasks is expected to be "+
			"a single literal map entry named 'array' of type LiteralCollection.")
	}

	if l.GetCollection() == nil {
		return nil, errors.Errorf(errors.BadSpecificationError, nodeId, "Output of array tasks of key 'array' "+
			"is of type [%v]. LiteralCollection is expected.", reflect.TypeOf(l.GetValue()))
	}

	literals := l.GetCollection().Literals
	if idx >= len(literals) {
		return nil, errors.Errorf(errors.OutputsNotFoundError, nodeId, "Failed to find [%v[%v].%v]",
			nodeId, idx, varName)
	}

	return literals[idx], nil
}

func resolveSingleOutput(ctx context.Context, store storage.ProtobufStore, nodeId string, outputsFileRef storage.DataReference,
	varName string) (*core.Literal, error) {

	d := &core.LiteralMap{}
	if err := store.ReadProtobuf(ctx, outputsFileRef, d); err != nil {
		return nil, errors.Wrapf(errors.CausedByError, nodeId, err, "Failed to GetPrevious data from dataDir [%v]",
			outputsFileRef)
	}

	if d.Literals == nil {
		return nil, errors.Errorf(errors.OutputsNotFoundError, nodeId,
			"Outputs not found at [%v]", outputsFileRef)
	}

	l, ok := d.Literals[varName]
	if !ok {
		return nil, errors.Errorf(errors.OutputsNotFoundError, nodeId,
			"Failed to find [%v].[%v]", nodeId, varName)
	}

	return l, nil
}

// Creates a simple output resolver that expects an outputs.pb at the data directory of the node.
func NewRemoteFileOutputResolver(store *storage.DataStore) OutputResolver {
	return remoteFileOutputResolver{
		store: store,
	}
}
