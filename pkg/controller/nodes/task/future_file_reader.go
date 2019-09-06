package task

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"github.com/pkg/errors"
)

// TODO this file exists only until we need to support dynamic nodes instead of closure.
// Once closure migration is done, this file should be deleted.
const implicitFutureFileName = "futures.pb"

type FutureFileReader struct {
	loc   storage.DataReference
	store *storage.DataStore
}

func (f FutureFileReader) Exists(ctx context.Context) (bool, error) {
	metadata, err := f.store.Head(ctx, f.loc)
	// If no futures file produced, then declare success and return.
	if err != nil {
		logger.Warnf(ctx, "Failed to read futures file. Error: %v", err)
		return false, errors.Wrap(err, "Failed to do HEAD on futures file.")
	}
	return metadata.Exists(), nil
}

func (f FutureFileReader) Read(ctx context.Context) (*core.DynamicJobSpec, error) {
	djSpec := &core.DynamicJobSpec{}
	if err := f.store.ReadProtobuf(ctx, f.loc, djSpec); err != nil {
		logger.Warnf(ctx, "Failed to read futures file. Error: %v", err)
		return nil, errors.Wrap(err, "Failed to read futures protobuf file.")
	}

	return djSpec, nil
}

func NewRemoteFutureFileReader(ctx context.Context, dataDir storage.DataReference, store *storage.DataStore) (FutureFileReader, error) {
	loc, err := store.ConstructReference(ctx, dataDir, implicitFutureFileName)
	if err != nil {
		logger.Warnf(ctx, "Failed to construct data path for futures file. Error: %v", err)
		return FutureFileReader{}, err
	}
	return FutureFileReader{
		loc:   loc,
		store: store,
	}, nil
}
