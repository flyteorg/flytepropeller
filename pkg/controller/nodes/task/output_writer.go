package task

import (
	"context"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/storage"
)

const outputsFile = "outputs.pb"
const errFile = "error.pb"

type outputFilePaths struct {
	dataDir storage.DataReference
	outPath storage.DataReference
	errPath storage.DataReference
}

func (o outputFilePaths) GetOutputPrefixPath() storage.DataReference {
	return o.dataDir
}

func (o outputFilePaths) GetOutputPath() storage.DataReference {
	return o.outPath
}

func (o outputFilePaths) GetErrorPath() storage.DataReference {
	return o.errPath
}

type outputWriter struct {
	outputFilePaths
	outReader io.OutputReader
}

func (o *outputWriter) Put(ctx context.Context, reader io.OutputReader) error {
	o.outReader = reader
	return nil
}

func (o *outputWriter) GetReader() io.OutputReader {
	return o.outReader
}

func NewRemoteFileOutputPaths(ctx context.Context,  baseDir storage.DataReference, constructor storage.ReferenceConstructor) (outputFilePaths, error) {
	oPath, err := constructor.ConstructReference(ctx, baseDir, outputsFile)
	if err != nil {
		return outputFilePaths{}, err
	}

	errPath, err := constructor.ConstructReference(ctx, baseDir, errFile)
	if err != nil {
		return outputFilePaths{}, err
	}

	return outputFilePaths{
		dataDir: baseDir,
		outPath: oPath,
		errPath: errPath,
	}, nil
}

func NewRemoteFileOutputWriter(ctx context.Context, baseDir storage.DataReference, store *storage.DataStore) (*outputWriter, error) {
	p, err := NewRemoteFileOutputPaths(ctx, baseDir, store)
	if err != nil {
		return nil, err
	}
	return &outputWriter{
		outputFilePaths: p,
	}, nil
}
