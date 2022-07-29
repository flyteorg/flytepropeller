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

func TestActiveCRDOffloadStore(t *testing.T) {
	ctx := context.TODO()
	staticWorkflowData := v1alpha1.StaticWorkflowData{}

	t.Run("Happy", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.CRDOffloadStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&staticWorkflowData, nil)

		scope := promutils.NewTestScope()
		activeStore := NewActiveCRDOffloadStore(mockStore, scope)

		// Get from underlying CRDOffloadStore
		data, err := activeStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Get from cache
		data, err = activeStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})

	t.Run("Remove", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.CRDOffloadStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(&staticWorkflowData, nil)
		mockStore.OnRemoveMatch(mock.Anything, mock.Anything).Return(nil)

		scope := promutils.NewTestScope()
		activeStore := NewActiveCRDOffloadStore(mockStore, scope)

		// Get from underlying CRDOffloadStore
		data, err := activeStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 1)

		// Remove from underlying CRDOffloadStore
		err = activeStore.Remove(ctx, "foo")
		assert.NoError(t, err)
		mockStore.AssertNumberOfCalls(t, "Remove", 1)

		// Get from cache
		data, err = activeStore.Get(ctx, "foo")
		assert.NoError(t, err)
		assert.True(t, reflect.DeepEqual(staticWorkflowData, *data))
		mockStore.AssertNumberOfCalls(t, "Get", 2)
	})

	t.Run("UnderlyingError", func(t *testing.T) {
		// initialize mocks
		mockStore := &mocks.CRDOffloadStore{}
		mockStore.OnGetMatch(mock.Anything, mock.Anything).Return(nil, fmt.Errorf("foo"))

		scope := promutils.NewTestScope()
		activeStore := NewActiveCRDOffloadStore(mockStore, scope)

		// Get from underlying CRDOffloadStore
		data, err := activeStore.Get(ctx, "foo")
		assert.Error(t, err)
		assert.Nil(t, data)
		mockStore.AssertNumberOfCalls(t, "Get", 1)
	})
}
