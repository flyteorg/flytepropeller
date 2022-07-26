package staticobjstore

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/static"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
)

//go:generate mockery -all -output=mocks -case=underscore

// WorkflowStaticObjectStore store interface provides an abstraction of accessing the actual WorkflowStaticObjectStore object.
type WorkflowStaticObjectStore interface {
	Get(ctx context.Context, wf *v1alpha1.FlyteWorkflow) (*static.WorkflowStaticExecutionObj, error)
	Remove(ctx context.Context, wf *v1alpha1.FlyteWorkflow)
}
