package workflowclosurestore

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flytepropeller/pkg/controller/workflowclosurestore/mocks"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestLruWfClosureStore(t *testing.T) {
	ctx := context.TODO()
	workflowClosure := core.CompiledWorkflowClosure{}

	t.Run("Happy", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WorkflowClosureStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&workflowClosure, nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWorkflowClosureStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WorkflowClosureStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Get from cache
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})

	t.Run("Remove", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WorkflowClosureStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&workflowClosure, nil)
		mockStore.OnRemoveMatch(mock.Anything, mock.Anything).Return(nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWorkflowClosureStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WorkflowClosureStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Remove from underlying WorkflowClosureStore
		err = lruStore.Remove(ctx, "foo")
		assert.NoError(t, err)
		mockStore.AssertNumberOfCalls(t, "Remove", 1)

		// Get from cache
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 2)
	})

	t.Run("Eviction", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WorkflowClosureStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&workflowClosure, nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWorkflowClosureStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WorkflowClosureStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Get from cache
		data, err = lruStore.Get(ctx, "bar")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 2)

		// Get eviction
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(workflowClosure, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 3)
	})

	t.Run("UnderlyingError", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WorkflowClosureStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("foo"))

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWorkflowClosureStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WorkflowClosureStore
		data, err := lruStore.Get(ctx, "foo")
		assert.Error(t, err)
		assert.Nil(t, data)
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})
}
