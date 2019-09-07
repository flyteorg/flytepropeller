package task

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/storage"
)

const outputsFile = "outputs.pb"
const errFile = "error.pb"

type OutputFilePaths struct {
	dataDir storage.DataReference
	outPath storage.DataReference
	errPath storage.DataReference
}

func (o OutputFilePaths) GetOutputPrefixPath() storage.DataReference {
	return o.dataDir
}

func (o OutputFilePaths) GetOutputPath() storage.DataReference {
	return o.outPath
}

func (o OutputFilePaths) GetErrorPath() storage.DataReference {
	return o.errPath
}

type OutputWriter struct {
	OutputFilePaths
	outReader io.OutputReader
}

func (o *OutputWriter) Put(ctx context.Context, reader io.OutputReader) error {
	o.outReader = reader
	return nil
}

func (o *OutputWriter) GetReader() io.OutputReader {
	return o.outReader
}

func NewRemoteFileOutputPaths(ctx context.Context, baseDir storage.DataReference, constructor storage.ReferenceConstructor) (OutputFilePaths, error) {
	oPath, err := constructor.ConstructReference(ctx, baseDir, outputsFile)
	if err != nil {
		return OutputFilePaths{}, err
	}

	errPath, err := constructor.ConstructReference(ctx, baseDir, errFile)
	if err != nil {
		return OutputFilePaths{}, err
	}

	return OutputFilePaths{
		dataDir: baseDir,
		outPath: oPath,
		errPath: errPath,
	}, nil
}

func NewRemoteFileOutputWriter(ctx context.Context, baseDir storage.DataReference, store *storage.DataStore) (*OutputWriter, error) {
	p, err := NewRemoteFileOutputPaths(ctx, baseDir, store)
	if err != nil {
		return nil, err
	}
	return &OutputWriter{
		OutputFilePaths: p,
	}, nil
}
