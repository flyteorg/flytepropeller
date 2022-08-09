package wfcrdfieldsstore

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/flyteorg/flytepropeller/pkg/controller/wfcrdfieldsstore/mocks"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestLruWfClosureStore(t *testing.T) {
	ctx := context.TODO()
	wfClosureCrdFields := k8s.WfClosureCrdFields{}

	t.Run("Happy", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WfClosureCrdFieldsStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&wfClosureCrdFields, nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWfClosureCrdFieldsStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WfClosureCrdFieldsStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Get from cache
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})

	t.Run("Remove", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WfClosureCrdFieldsStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&wfClosureCrdFields, nil)
		mockStore.OnRemoveMatch(mock.Anything, mock.Anything).Return(nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWfClosureCrdFieldsStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WfClosureCrdFieldsStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Remove from underlying WfClosureCrdFieldsStore
		err = lruStore.Remove(ctx, "foo")
		assert.NoError(t, err)
		mockStore.AssertNumberOfCalls(t, "Remove", 1)

		// Get from cache
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 2)
	})

	t.Run("Eviction", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WfClosureCrdFieldsStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&wfClosureCrdFields, nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWfClosureCrdFieldsStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WfClosureCrdFieldsStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Get from cache
		data, err = lruStore.Get(ctx, "bar")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 2)

		// Get eviction
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(wfClosureCrdFields, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 3)
	})

	t.Run("UnderlyingError", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.WfClosureCrdFieldsStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("foo"))

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUWfClosureCrdFieldsStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// Get from underlying WfClosureCrdFieldsStore
		data, err := lruStore.Get(ctx, "foo")
		assert.Error(t, err)
		assert.Nil(t, data)
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})
}
