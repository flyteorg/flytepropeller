package task

import (
	"context"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flytestdlib/logger"
	"k8s.io/apimachinery/pkg/util/cache"
)

var defaultExpirationDuration = time.Minute * 5

type BarrierKey = string

type PluginCallLog struct {
	PluginState        []byte
	PluginStateVersion uint32
	PluginTransition   core.Transition
}

type BarrierTransition struct {
	BarrierClockTick uint32
	CallLog          PluginCallLog
}

var NoBarrierTransition = BarrierTransition{BarrierClockTick: 0}

type barrier struct {
	barrierTransitions cache.LRUExpireCache
}

func (b *barrier) RecordBarrierTransition(ctx context.Context, k BarrierKey, bt BarrierTransition) {
	b.barrierTransitions.Add(k, bt, defaultExpirationDuration)
}

func (b *barrier) GetPreviousBarrierTransition(ctx context.Context, k BarrierKey) BarrierTransition {
	if v, ok := b.barrierTransitions.Get(k); ok {
		f, casted := v.(BarrierTransition)
		if !casted {
			logger.Errorf(ctx, "Failed to cast recorded value to BarrierTransition")
			return NoBarrierTransition
		}
		return f
	}
	return NoBarrierTransition
}
