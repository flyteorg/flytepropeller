package nodes

import (
	"context"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"
)

type remoteFileInputReader struct {
	store           *storage.DataStore
	inputPath       storage.DataReference
	inputPrefixPath storage.DataReference
}

func (r *remoteFileInputReader) GetInputPrefixPath() storage.DataReference {
	return r.inputPrefixPath
}

func (r *remoteFileInputReader) GetInputPath() storage.DataReference {
	return r.inputPath
}

func (r *remoteFileInputReader) Get(ctx context.Context) (*core.LiteralMap, error) {
	in := &core.LiteralMap{}
	if err := r.store.ReadProtobuf(ctx, r.inputPath, in); err != nil {
		return nil, err
	}
	return in, nil
}
