package controller

import (
	resourceRequest2 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/controller/resourceRequest"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, resourceRequest2.Add)
}
