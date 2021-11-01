package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	randomShardStrategy = RandomShardStrategy{
		enableUncoveredReplica: false,
		podCount:               3,
	}

	randomShardStrategyUncovered = RandomShardStrategy{
		enableUncoveredReplica: false,
		podCount:               3,
	}
)

func TestComputeKeyRange(t *testing.T) {
	keyspaceSize := 32
	for podCount := 1; podCount < keyspaceSize; podCount++ {
		keysCovered := 0
		minKeyRangeSize := keyspaceSize / podCount
		for podIndex := 0; podIndex < podCount; podIndex++ {
			startIndex, endIndex := computeKeyRange(keyspaceSize, podCount, podIndex)

			rangeSize := endIndex - startIndex
			keysCovered += rangeSize
			assert.True(t, rangeSize - minKeyRangeSize >= 0)
			assert.True(t, rangeSize - minKeyRangeSize <= 1)
		}

		assert.Equal(t, keyspaceSize, keysCovered)
	}
}

func TestGetPodCount(t *testing.T) {
	/*t.Run("no-labels", func(t *testing.T) {
		w := &v1alpha1.FlyteWorkflow{}
		assert.Empty(t, w.Labels)
		assert.False(t, HasCompletedLabel(w))
		SetCompletedLabel(w, n)
		assert.NotEmpty(t, w.Labels)
		v, ok := w.Labels[workflowTerminationStatusKey]
		assert.True(t, ok)
		assert.Equal(t, workflowTerminatedValue, v)
		assert.True(t, HasCompletedLabel(w))
	})*/
}
