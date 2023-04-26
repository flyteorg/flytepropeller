package array

import (
	"context"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"
)

type staticInputReader struct {
	io.InputFilePaths
	input *idlcore.LiteralMap
}

func (i staticInputReader) Get(_ context.Context) (*idlcore.LiteralMap, error) {
	return i.input, nil
}

func newStaticInputReader(inputPaths io.InputFilePaths, input *idlcore.LiteralMap) staticInputReader {
	return staticInputReader{
		InputFilePaths: inputPaths,
		input:          input,
	}
}

func constructLiteralMap(ctx context.Context, inputReader io.InputReader, index int) (*idlcore.LiteralMap, error) {
	inputs, err := inputReader.Get(ctx)
	if err != nil {
		return nil, err
	}

	literals := make(map[string]*idlcore.Literal)
	for name, literal := range inputs.Literals {
		if literalCollection := literal.GetCollection(); literalCollection != nil {
			literals[name] = literalCollection.Literals[index]
		}
	}

	return &idlcore.LiteralMap{
		Literals: literals,
	}, nil
}
