package controller

import (
	"context"
	config2 "github.com/lyft/flytepropeller/k8s-scheduler/pkg/controller/config"

	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// AddToManagerFuncs is a list of functions to add all Controllers to the Manager
var AddToManagerFuncs []func(context.Context, manager.Manager, config2.RuntimeConfig) error

// AddToManager adds all Controllers to the Manager
func AddToManager(ctx context.Context, m manager.Manager, runtimeCfg config2.RuntimeConfig) error {
	for _, f := range AddToManagerFuncs {
		if err := f(ctx, m, runtimeCfg); err != nil {
			return err
		}
	}
	return nil
}
