package workflowstore

import (
	"fmt"

	"github.com/pkg/errors"
)

var ErrStaleWorkflowError = fmt.Errorf("stale Workflow Found error")
var ErrWorkflowNotFound = fmt.Errorf("workflow not-found error")
var ErrWorkflowToLarge = fmt.Errorf("workflow too large")

func IsNotFound(err error) bool {
	return errors.Cause(err) == ErrWorkflowNotFound
}

func IsWorkflowStale(err error) bool {
	return errors.Cause(err) == ErrStaleWorkflowError
}

func IsWorkflowTooLarge(err error) bool {
	return errors.Cause(err) == ErrWorkflowToLarge
}
