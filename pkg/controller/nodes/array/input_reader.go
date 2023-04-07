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

/*func ConstructStaticInputReaders(inputPaths io.InputFilePaths, inputs []*idlcore.Literal, inputName string) []io.InputReader {
	inputReaders := make([]io.InputReader, 0, len(inputs))
	for i := 0; i < len(inputs); i++ {
		inputReaders = append(inputReaders, NewStaticInputReader(inputPaths, &idlcore.LiteralMap{
			Literals: map[string]*idlcore.Literal{
				inputName: inputs[i],
			},
		}))
	}

	return inputReaders
}*/
