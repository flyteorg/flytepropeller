package catalog

import (
	"context"
	"fmt"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/catalog/datacatalog"
)

//go:generate mockery -all -case=underscore
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

func NewCatalogClient(ctx context.Context) (Client, error) {
	catalogConfig := GetConfig()

	var catalogClient Client
	var err error
	switch catalogConfig.Type {
	case DataCatalogType:
		catalogClient, err = datacatalog.NewDataCatalog(ctx, catalogConfig.Endpoint, catalogConfig.Insecure)
		if err != nil {
			return nil, err
		}
	case NoOpDiscoveryType, "":
		catalogClient = NOOPCatalog{}
	default:
		return nil, fmt.Errorf("no such catalog type available: %s", catalogConfig.Type)
	}

	logger.Infof(context.Background(), "Created Catalog client, type: %v", catalogConfig.Type)
	return catalogClient, nil
}
