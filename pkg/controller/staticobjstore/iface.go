package staticobjstore

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/static"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

// FlyteWorkflow store interface provides an abstraction of accessing the actual FlyteWorkflow object.
type WorkflowStaticObjectStore interface {
	Get(ctx context.Context, wf *v1alpha1.FlyteWorkflow) (*static.WorkflowStaticExecutionObj, error)
	Remove(ctx context.Context, wf *v1alpha1.FlyteWorkflow) error
}
