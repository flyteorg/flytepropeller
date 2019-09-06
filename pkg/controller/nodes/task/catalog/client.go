package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
)

//
type Metadata struct {
	WorkflowExecutionIdentifier *core.WorkflowExecutionIdentifier
	TaskExecutionIdentifier     *core.TaskExecutionIdentifier
}

type Key struct {
	Identifier     core.Identifier
	CacheVersion   string
	TypedInterface core.TypedInterface
	InputReader    io.InputReader
}

func (k Key) String() string {
	return fmt.Sprintf("%v:%v", k.Identifier, k.CacheVersion)
}

type Client interface {
	Get(ctx context.Context, key Key) (io.OutputReader, error)
	Put(ctx context.Context, key Key, reader io.OutputReader, metadata Metadata) error
}