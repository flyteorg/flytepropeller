package catalog

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

type NOOPCatalog struct {
}

func (n NOOPCatalog) Get(ctx context.Context, key Key) (io.OutputReader, error) {
	return nil, nil
}

func (n NOOPCatalog) Put(ctx context.Context, key Key, reader io.OutputReader, metadata Metadata) error {
	return nil
}
