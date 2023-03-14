package k8s

import (
	"context"
	"fmt"

	pluginsCore "github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/flyteorg/flytestdlib/logger"
)

var _ k8s.PluginContext = &pluginContext{}

type pluginContext struct {
	pluginsCore.TaskExecutionContext
	// Lazily creates a buffered outputWriter, overriding the input outputWriter.
	ow             *ioutils.BufferedOutputWriter
	k8sPluginState *k8s.PluginState
}

// Provides an output sync of type io.OutputWriter
func (p *pluginContext) OutputWriter() io.OutputWriter {
	logger.Debugf(context.TODO(), "K8s plugin is requesting output writer, creating a buffer.")
	buf := ioutils.NewBufferedOutputWriter(context.TODO(), p.TaskExecutionContext.OutputWriter())
	p.ow = buf
	return buf
}

// TODO @hamersaw docs
type pluginStateReader struct {
	k8sPluginState *k8s.PluginState
}

// TODO @hamersaw docs
func (p pluginStateReader) GetStateVersion() uint8 {
	return 0
}

// TODO @hamersaw docs
func (p pluginStateReader) Get(t interface{}) (stateVersion uint8, err error) {
	if pointer, ok := t.(*k8s.PluginState); ok {
		*pointer = *p.k8sPluginState
	} else {
		return 0, fmt.Errorf("unexpected type when reading plugin state")
	}

	return 0, nil
}

// TODO @hamersaw docs
func (p *pluginContext) PluginStateReader() pluginsCore.PluginStateReader {
	return pluginStateReader{
		k8sPluginState: p.k8sPluginState,
	}
}

func newPluginContext(tCtx pluginsCore.TaskExecutionContext, k8sPluginState *k8s.PluginState) *pluginContext {
	return &pluginContext{
		TaskExecutionContext: tCtx,
		ow:                   nil,
		k8sPluginState:       k8sPluginState,
	}
}
