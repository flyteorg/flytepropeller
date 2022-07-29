package crdoffloadstore

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/controller/crdoffloadstore/mocks"

	"github.com/flyteorg/flytestdlib/promutils"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
)

func TestLruCRDOffloadStore(t *testing.T) {
	ctx := context.TODO()
	staticWorkflowData := v1alpha1.StaticWorkflowData{}

	t.Run("Happy", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.CRDOffloadStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&staticWorkflowData, nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUCRDOffloadStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// test Get from underlying CRDOffloadStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// test Get from cache
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})

	t.Run("Eviction", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.CRDOffloadStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&staticWorkflowData, nil)

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUCRDOffloadStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// test Get from underlying CRDOffloadStore
		data, err := lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// test Get from cache
		data, err = lruStore.Get(ctx, "bar")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 2)

		// test Get eviction
		data, err = lruStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 3)
	})

	t.Run("UnderlyingError", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.CRDOffloadStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("foo"))

		scope := promutils.NewTestScope()
		lruStore, err := NewLRUCRDOffloadStore(mockStore, 1, scope)
		assert.NoError(t, err)

		// test Get from underlying CRDOffloadStore
		data, err := lruStore.Get(ctx, "foo")
		assert.Error(t, err)
		assert.Nil(t, data)
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})
}
