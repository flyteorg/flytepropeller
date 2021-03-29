package task

import (
	"context"
	pluginMachinery "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery"
	pluginCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flytestdlib/logger"
	"time"
)

type SleeperPlugin struct {

}

func (s SleeperPlugin) GetID() string {
	return "sleep"
}

func (s SleeperPlugin) GetProperties() pluginCore.PluginProperties {
	return pluginCore.PluginProperties{}
}

func (s SleeperPlugin) Handle(ctx context.Context, tCtx pluginCore.TaskExecutionContext) (pluginCore.Transition, error) {
	logger.Infof(ctx, "Sleeper plugin invoked!")
	tk, err := tCtx.TaskReader().Read(ctx)
	if err != nil {
		return pluginCore.UnknownTransition, err
	}
	sleepTime := time.Millisecond * 1000
	if tk.GetConfig() != nil {
			v, ok := tk.GetConfig()["sleep"]
			if ok {
				i, err  := time.ParseDuration(v)
				if err == nil {
					sleepTime = i
				}
			}
	}
	logger.Infof(ctx, "Sleeping for %v", sleepTime)
	time.Sleep(sleepTime)
	return pluginCore.DoTransition(pluginCore.PhaseInfoSuccess(nil)), nil
}

func (s SleeperPlugin) Abort(ctx context.Context, tCtx pluginCore.TaskExecutionContext) error {
	return nil
}

func (s SleeperPlugin) Finalize(ctx context.Context, tCtx pluginCore.TaskExecutionContext) error {
	return nil
}

func init() {
	pluginMachinery.PluginRegistry().RegisterCorePlugin(pluginCore.PluginEntry{
		ID: "sleep",
		RegisteredTaskTypes: []pluginCore.TaskType{"sleep"},
		LoadPlugin: func(ctx context.Context, iCtx pluginCore.SetupContext) (pluginCore.Plugin, error) {
			return SleeperPlugin{}, nil
		},
		DefaultForTaskTypes: []pluginCore.TaskType{"sleep"},
	})
}
