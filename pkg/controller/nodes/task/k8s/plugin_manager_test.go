package k8s

import (
	"context"
	"fmt"
	"testing"

	"github.com/lyft/flyteplugins/go/tasks/flytek8s/config"

	"github.com/lyft/flytestdlib/promutils"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pluginsCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	pluginsCoreMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core/mocks"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	pluginsk8sMock "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s/mocks"

	"github.com/lyft/flytestdlib/storage"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/lyft/flytepropeller/pkg/controller/executors/mocks"
)

type k8sSampleHandler struct {
}

func (k8sSampleHandler) BuildResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionContext) (k8s.Resource, error) {
	panic("implement me")
}

func (k8sSampleHandler) BuildIdentityResource(ctx context.Context, taskCtx pluginsCore.TaskExecutionMetadata) (k8s.Resource, error) {
	panic("implement me")
}

func (k8sSampleHandler) GetTaskPhase(ctx context.Context, pluginContext k8s.PluginContext, resource k8s.Resource) (pluginsCore.PhaseInfo, error) {
	panic("implement me")
}

func ExampleNewPluginManager() {
	sCtx := &pluginsCoreMock.SetupContext{}
	fakeKubeClient := mocks.NewFakeKubeClient()
	sCtx.On("KubeClient").Return(fakeKubeClient)
	sCtx.On("OwnerKind").Return("test")
	sCtx.On("EnqueueOwner").Return(pluginsCore.EnqueueOwner(func(name k8stypes.NamespacedName) error { return nil }))
	sCtx.On("MetricsScope").Return(promutils.NewTestScope())
	exec, err := NewPluginManager(
		context.TODO(),
		sCtx,
		k8s.PluginEntry{
			ID:                  "SampleHandler",
			RegisteredTaskTypes: []pluginsCore.TaskType{"container"},
			ResourceToWatch:     &v1.Pod{},
			Plugin:              k8sSampleHandler{},
		})
	if err == nil {
		fmt.Printf("Created executor: %v\n", exec.GetID())
	} else {
		fmt.Printf("Error in creating executor: %s\n", err.Error())
	}

	// Output:
	// Created executor: SampleHandler
}

func getMockTaskContext() pluginsCore.TaskExecutionContext {
	taskExecutionContext := &pluginsCoreMock.TaskExecutionContext{}
	taskExecutionContext.On("TaskExecutionMetadata").Return(getMockTaskExecutionMetadata())

	customStateReader := &pluginsCoreMock.PluginStateReader{}
	customStateReader.On("Get", mock.Anything).Return(nil)
	taskExecutionContext.On("CustomStateReader").Return(customStateReader)

	customStateWriter := &pluginsCoreMock.PluginStateWriter{}
	customStateWriter.On("Put", mock.Anything).Return(nil)
	taskExecutionContext.On("CustomStateWriter").Return(customStateWriter)
	return taskExecutionContext
}

func getMockTaskExecutionMetadata() pluginsCore.TaskExecutionMetadata {
	taskExecutionMetadata := &pluginsCoreMock.TaskExecutionMetadata{}
	taskExecutionMetadata.On("GetNamespace").Return("ns")
	taskExecutionMetadata.On("GetAnnotations").Return(map[string]string{"aKey": "aVal"})
	taskExecutionMetadata.On("GetLabels").Return(map[string]string{"lKey": "lVal"})
	taskExecutionMetadata.On("GetOwnerReference").Return(v12.OwnerReference{Name: "x"})
	taskExecutionMetadata.On("GetOutputsFile").Return(storage.DataReference("outputs"))
	taskExecutionMetadata.On("GetInputsFile").Return(storage.DataReference("inputs"))
	taskExecutionMetadata.On("GetErrorFile").Return(storage.DataReference("error"))

	id := &pluginsCoreMock.TaskExecutionID{}
	id.On("GetGeneratedName").Return("test")
	id.On("GetID").Return(core.TaskExecutionIdentifier{})
	taskExecutionMetadata.On("GetTaskExecutionID").Return(id)
	return taskExecutionMetadata
}

func dummySetupContext(fakeClient client.Client) pluginsCore.SetupContext {
	setupContext := &pluginsCoreMock.SetupContext{}
	var enqueueOwnerFunc = pluginsCore.EnqueueOwner(func(ownerId k8stypes.NamespacedName) error { return nil })
	setupContext.On("EnqueueOwner").Return(enqueueOwnerFunc)

	kubeClient := &pluginsCoreMock.KubeClient{}
	kubeClient.On("GetClient").Return(fakeClient)
	setupContext.On("KubeClient").Return(kubeClient)

	setupContext.On("OwnerKind").Return("x")
	setupContext.On("MetricsScope").Return(promutils.NewTestScope())

	return setupContext
}

func TestK8sTaskExecutor_StartTask(t *testing.T) {
	ctx := context.TODO()
	tctx := getMockTaskContext()
	/*var tmpl *core.TaskTemplate
	var inputs *core.LiteralMap*/

	t.Run("jobQueued", func(t *testing.T) {
		// common setup code
		mockResourceHandler := &pluginsk8sMock.Plugin{}
		mockResourceHandler.On("BuildResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)
		// evRecorder := &mocks2.EventRecorder{}
		fakeClient := fake.NewFakeClient()
		pluginManager, err := NewPluginManager(ctx, dummySetupContext(fakeClient), k8s.PluginEntry{
			ID:              "x",
			ResourceToWatch: &v1.Pod{},
			Plugin:          mockResourceHandler,
		})
		assert.NoError(t, err)

		/*evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
		mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return e.Phase == core.TaskExecution_QUEUED })).Return(nil)*/

		transition, err := pluginManager.Handle(ctx, tctx)
		assert.NoError(t, err)
		assert.NotNil(t, transition)
		transitionInfo := transition.Info()
		assert.NotNil(t, transitionInfo)
		assert.Equal(t, pluginsCore.PhaseQueued, transitionInfo.Phase())
		createdPod := &v1.Pod{}

		AddObjectMetadata(tctx.TaskExecutionMetadata(), createdPod, &config.K8sPluginConfig{})
		assert.NoError(t, fakeClient.Get(ctx, k8stypes.NamespacedName{Namespace: tctx.TaskExecutionMetadata().GetNamespace(),
			Name: tctx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName()}, createdPod))
		assert.Equal(t, tctx.TaskExecutionMetadata().GetTaskExecutionID().GetGeneratedName(), createdPod.Name)
		assert.NoError(t, fakeClient.Delete(ctx, createdPod))
	})
	/*
		t.Run("jobAlreadyExists", func(t *testing.T) {
			// common setup code
			mockResourceHandler := &mocks.K8sResourceHandler{}
			evRecorder := &mocks2.EventRecorder{}
			k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
			mockResourceHandler.On("BuildResource", mock.Anything, tctx, tmpl, inputs).Return(&v1.Pod{}, nil)
			err := k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder))
			assert.NoError(t, err)

			expectedNewStatus := types.TaskStatusQueued
			mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(expectedNewStatus, nil, nil)

			evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
				mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return e.Phase == core.TaskExecution_QUEUED })).Return(nil)

			status, err := k.StartTask(ctx, tctx, nil, nil)
			assert.NoError(t, err)
			assert.Nil(t, status.State)
			assert.Equal(t, types.TaskPhaseQueued, status.Phase)
			createdPod := &v1.Pod{}
			flytek8s.AddObjectMetadata(tctx, createdPod)
			assert.NoError(t, c.Get(ctx, k8stypes.NamespacedName{Namespace: tctx.GetNamespace(), Name: tctx.GetTaskExecutionID().GetGeneratedName()}, createdPod))
			assert.Equal(t, tctx.GetTaskExecutionID().GetGeneratedName(), createdPod.Name)
		})

		t.Run("jobDifferentTerminalState", func(t *testing.T) {
			// common setup code
			mockResourceHandler := &mocks.K8sResourceHandler{}
			evRecorder := &mocks2.EventRecorder{}
			k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
			mockResourceHandler.On("BuildResource", mock.Anything, tctx, tmpl, inputs).Return(&v1.Pod{}, nil)
			err := k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder))
			assert.NoError(t, err)

			expectedNewStatus := types.TaskStatusQueued
			mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(expectedNewStatus, nil, nil)

			evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
				mock.MatchedBy(func(e *event.TaskExecutionEvent) bool {
					return e.Phase == core.TaskExecution_QUEUED
				})).Return(&eventErrors.EventError{Code: eventErrors.EventAlreadyInTerminalStateError,
				Cause: errors.New("already exists"),
			})

			status, err := k.StartTask(ctx, tctx, nil, nil)
			assert.NoError(t, err)
			assert.Nil(t, status.State)
			assert.Equal(t, types.TaskPhasePermanentFailure, status.Phase)
		})

		t.Run("jobQuotaExceeded", func(t *testing.T) {
			// common setup code
			mockResourceHandler := &mocks.K8sResourceHandler{}
			evRecorder := &mocks2.EventRecorder{}
			k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
			mockResourceHandler.On("BuildResource", mock.Anything, tctx, tmpl, inputs).Return(&v1.Pod{}, nil)
			err := k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder))
			assert.NoError(t, err)

			evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
				mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return e.Phase == core.TaskExecution_QUEUED })).Return(nil)

			// override create to return quota exceeded
			mockRuntimeClient := mocks.NewMockRuntimeClient()
			mockRuntimeClient.CreateCb = func(ctx context.Context, obj runtime.Object) (err error) {
				return k8serrs.NewForbidden(schema.GroupResource{}, "", errors.New("exceeded quota"))
			}
			if err := flytek8s.InjectClient(mockRuntimeClient); err != nil {
				assert.NoError(t, err)
			}

			status, err := k.StartTask(ctx, tctx, nil, nil)
			assert.NoError(t, err)
			assert.Nil(t, status.State)
			assert.Equal(t, types.TaskPhaseNotReady, status.Phase)

			// reset the client back to fake client
			if err := flytek8s.InjectClient(fake.NewFakeClient()); err != nil {
				assert.NoError(t, err)
			}
		})

		t.Run("jobForbidden", func(t *testing.T) {
			// common setup code
			mockResourceHandler := &mocks.K8sResourceHandler{}
			evRecorder := &mocks2.EventRecorder{}
			k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
			mockResourceHandler.On("BuildResource", mock.Anything, tctx, tmpl, inputs).Return(&v1.Pod{}, nil)
			err := k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder))
			assert.NoError(t, err)

			evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
				mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return e.Phase == core.TaskExecution_FAILED })).Return(nil)

			// override create to return forbidden
			mockRuntimeClient := mocks.NewMockRuntimeClient()
			mockRuntimeClient.CreateCb = func(ctx context.Context, obj runtime.Object) (err error) {
				return k8serrs.NewForbidden(schema.GroupResource{}, "", nil)
			}
			if err := flytek8s.InjectClient(mockRuntimeClient); err != nil {
				assert.NoError(t, err)
			}

			status, err := k.StartTask(ctx, tctx, nil, nil)
			assert.NoError(t, err)
			assert.Nil(t, status.State)
			assert.Equal(t, types.TaskPhasePermanentFailure, status.Phase)

			// reset the client back to fake client
			if err := flytek8s.InjectClient(fake.NewFakeClient()); err != nil {
				assert.NoError(t, err)
			}
		})

	*/
}

/*
func TestK8sTaskExecutor_CheckTaskStatus(t *testing.T) {
	ctx := context.TODO()

	t.Run("phaseChange", func(t *testing.T) {

		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewPluginManager( k8s.PluginEntry{
			ID: "x",
			ResourceToWatch:&v1.Pod{},
			Plugin: mockResourceHandler,
		})
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)

		assert.NoError(t, k.Seup(ctx, createExecutorInitializationParams(t, evRecorder)))
		testPod := &v1.Pod{}
		testPod.SetName(tctx.GetTaskExecutionID().GetGeneratedName())
		testPod.SetNamespace(tctx.GetNamespace())
		testPod.SetOwnerReferences([]v12.OwnerReference{tctx.GetOwnerReference()})
		err := c.Delete(ctx, testPod)
		if err != nil {
			assert.True(t, k8serrs.IsNotFound(err))
		}

		assert.NoError(t, c.Create(ctx, testPod))
		defer func() {
			assert.NoError(t, c.Delete(ctx, testPod))
		}()

		info := &events.TaskEventInfo{}
		info.Logs = []*core.TaskLog{{Uri: "log1"}}
		expectedOldPhase := types.TaskPhaseQueued
		expectedNewStatus := types.TaskStatusRunning
		expectedNewStatus.PhaseVersion = uint32(1)
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(1))
		mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(expectedNewStatus, nil, nil)

		evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
			mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return true })).Return(nil)

		s, err := k.Get(ctx, tctx, nil)
		assert.Nil(t, s.State)
		assert.NoError(t, err)
		assert.Equal(t, expectedNewStatus, s)
	})


	t.Run("PhaseMismatch", func(t *testing.T) {

		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)

		assert.NoError(t, k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder)))
		testPod := &v1.Pod{}
		testPod.SetName(tctx.GetTaskExecutionID().GetGeneratedName())
		testPod.SetNamespace(tctx.GetNamespace())
		testPod.SetOwnerReferences([]v12.OwnerReference{tctx.GetOwnerReference()})
		err := c.Delete(ctx, testPod)
		if err != nil {
			assert.True(t, k8serrs.IsNotFound(err))
		}

		assert.NoError(t, c.Create(ctx, testPod))
		defer func() {
			assert.NoError(t, c.Delete(ctx, testPod))
		}()

		info := &events.TaskEventInfo{}
		info.Logs = []*core.TaskLog{{Uri: "log1"}}
		expectedOldPhase := types.TaskPhaseRunning
		expectedNewStatus := types.TaskStatusSucceeded
		expectedNewStatus.PhaseVersion = uint32(1)
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(1))
		mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(expectedNewStatus, nil, nil)

		evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
			mock.MatchedBy(func(e *event.TaskExecutionEvent) bool {
				return e.Phase == core.TaskExecution_SUCCEEDED
			})).Return(&eventErrors.EventError{Code: eventErrors.EventAlreadyInTerminalStateError,
			Cause: errors.New("already exists"),
		})

		s, err := k.CheckTaskStatus(ctx, tctx, nil)
		assert.NoError(t, err)
		assert.Nil(t, s.State)
		assert.Equal(t, types.TaskPhasePermanentFailure, s.Phase)
	})

	t.Run("noChange", func(t *testing.T) {

		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)

		assert.NoError(t, k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder)))
		testPod := &v1.Pod{}
		testPod.SetName(tctx.GetTaskExecutionID().GetGeneratedName())
		testPod.SetNamespace(tctx.GetNamespace())
		testPod.SetOwnerReferences([]v12.OwnerReference{tctx.GetOwnerReference()})
		err := c.Delete(ctx, testPod)
		if err != nil {
			assert.True(t, k8serrs.IsNotFound(err))
		}

		assert.NoError(t, c.Create(ctx, testPod))
		defer func() {
			assert.NoError(t, c.Delete(ctx, testPod))
		}()

		info := &events.TaskEventInfo{}
		info.Logs = []*core.TaskLog{{Uri: "log1"}}
		expectedOldPhase := types.TaskPhaseRunning
		expectedNewStatus := types.TaskStatusRunning
		expectedNewStatus.PhaseVersion = uint32(1)
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(1))
		mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(expectedNewStatus, nil, nil)

		s, err := k.CheckTaskStatus(ctx, tctx, nil)
		assert.Nil(t, s.State)
		assert.NoError(t, err)
		assert.Equal(t, expectedNewStatus, s)
	})

	t.Run("resourceNotFound", func(t *testing.T) {

		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)

		assert.NoError(t, k.Initialize(ctx, createExecutorInitializationParams(t, evRecorder)))
		_ = flytek8s.InitializeFake()

		info := &events.TaskEventInfo{}
		info.Logs = []*core.TaskLog{{Uri: "log1"}}
		expectedOldPhase := types.TaskPhaseRunning
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(0))

		evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
			mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return true })).Return(nil)

		s, err := k.CheckTaskStatus(ctx, tctx, nil)
		assert.Nil(t, s.State)
		assert.NoError(t, err)
		assert.Equal(t, types.TaskPhaseRetryableFailure, s.Phase, "Expected failure got %s", s.Phase.String())
	})

	t.Run("errorFileExit", func(t *testing.T) {

		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)

		params := createExecutorInitializationParams(t, evRecorder)
		store := params.DataStore
		assert.NoError(t, k.Initialize(ctx, params))
		testPod := &v1.Pod{}
		testPod.SetName(tctx.GetTaskExecutionID().GetGeneratedName())
		testPod.SetNamespace(tctx.GetNamespace())
		testPod.SetOwnerReferences([]v12.OwnerReference{tctx.GetOwnerReference()})

		err := c.Delete(ctx, testPod)
		if err != nil {
			assert.True(t, k8serrs.IsNotFound(err))
		}

		assert.NoError(t, c.Create(ctx, testPod))
		defer func() {
			assert.NoError(t, c.Delete(ctx, testPod))
		}()

		assert.NoError(t, store.WriteProtobuf(ctx, tctx.GetErrorFile(), storage.Options{}, &core.ErrorDocument{
			Error: &core.ContainerError{
				Kind:    core.ContainerError_NON_RECOVERABLE,
				Code:    "code",
				Message: "pleh",
			},
		}))

		info := &events.TaskEventInfo{}
		info.Logs = []*core.TaskLog{{Uri: "log1"}}
		expectedOldPhase := types.TaskPhaseQueued
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(0))
		mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(types.TaskStatusSucceeded, nil, nil)

		evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
			mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return true })).Return(nil)

		s, err := k.CheckTaskStatus(ctx, tctx, nil)
		assert.Nil(t, s.State)
		assert.NoError(t, err)
		assert.Equal(t, types.TaskPhasePermanentFailure, s.Phase)
	})

	t.Run("errorFileExitRecoverable", func(t *testing.T) {
		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)

		params := createExecutorInitializationParams(t, evRecorder)
		store := params.DataStore

		assert.NoError(t, k.Initialize(ctx, params))
		testPod := &v1.Pod{}
		testPod.SetName(tctx.GetTaskExecutionID().GetGeneratedName())
		testPod.SetNamespace(tctx.GetNamespace())
		testPod.SetOwnerReferences([]v12.OwnerReference{tctx.GetOwnerReference()})

		err := c.Delete(ctx, testPod)
		if err != nil {
			assert.True(t, k8serrs.IsNotFound(err))
		}

		assert.NoError(t, c.Create(ctx, testPod))
		defer func() {
			assert.NoError(t, c.Delete(ctx, testPod))
		}()

		assert.NoError(t, store.WriteProtobuf(ctx, tctx.GetErrorFile(), storage.Options{}, &core.ErrorDocument{
			Error: &core.ContainerError{
				Kind:    core.ContainerError_RECOVERABLE,
				Code:    "code",
				Message: "pleh",
			},
		}))

		info := &events.TaskEventInfo{}
		info.Logs = []*core.TaskLog{{Uri: "log1"}}
		expectedOldPhase := types.TaskPhaseQueued
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(0))
		mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(types.TaskStatusSucceeded, nil, nil)

		evRecorder.On("RecordTaskEvent", mock.MatchedBy(func(c context.Context) bool { return true }),
			mock.MatchedBy(func(e *event.TaskExecutionEvent) bool { return e.Phase == core.TaskExecution_FAILED })).Return(nil)

		s, err := k.CheckTaskStatus(ctx, tctx, nil)
		assert.Nil(t, s.State)
		assert.NoError(t, err)
		assert.Equal(t, types.TaskPhaseRetryableFailure, s.Phase)
	})

	t.Run("nodeGetsDeleted", func(t *testing.T) {
		evRecorder := &mocks2.EventRecorder{}
		tctx := getMockTaskContext()
		mockResourceHandler := &mocks.K8sResourceHandler{}
		k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)
		mockResourceHandler.On("BuildIdentityResource", mock.Anything, tctx).Return(&v1.Pod{}, nil)
		params := createExecutorInitializationParams(t, evRecorder)
		assert.NoError(t, k.Initialize(ctx, params))
		testPod := &v1.Pod{}
		testPod.SetName(tctx.GetTaskExecutionID().GetGeneratedName())
		testPod.SetNamespace(tctx.GetNamespace())
		testPod.SetOwnerReferences([]v12.OwnerReference{tctx.GetOwnerReference()})

		testPod.SetFinalizers([]string{"test_finalizer"})

		// Ensure that the pod is not there
		err := c.Delete(ctx, testPod)
		assert.True(t, k8serrs.IsNotFound(err))

		// Add a deletion timestamp to the pod definition and then create it
		tt := time.Now()
		k8sTime := v12.Time{
			Time: tt,
		}
		testPod.SetDeletionTimestamp(&k8sTime)
		assert.NoError(t, c.Create(ctx, testPod))

		// Make sure that the phase doesn't change so no events are recorded
		expectedOldPhase := types.TaskPhaseQueued
		tctx.On("GetPhase").Return(expectedOldPhase)
		tctx.On("GetPhaseVersion").Return(uint32(0))
		mockResourceHandler.On("GetTaskStatus", mock.Anything, mock.Anything, mock.MatchedBy(func(o *v1.Pod) bool { return true })).Return(types.TaskStatusQueued, nil, nil)

		s, err := k.CheckTaskStatus(ctx, tctx, nil)
		assert.Nil(t, s.State)
		assert.NoError(t, err)
		assert.Equal(t, types.TaskPhaseRetryableFailure, s.Phase)
	})
}

func TestK8sTaskExecutor_HandleTaskSuccess(t *testing.T) {
	ctx := context.TODO()

	tctx := getMockTaskContext()
	mockResourceHandler := &mocks.K8sResourceHandler{}
	k := flytek8s.NewK8sTaskExecutorForResource("x", &v1.Pod{}, mockResourceHandler, time.Second)

	t.Run("no-errorfile", func(t *testing.T) {
		assert.NoError(t, k.Initialize(ctx, createExecutorInitializationParams(t, nil)))
		s, err := k.HandleTaskSuccess(ctx, tctx)
		assert.NoError(t, err)
		assert.Equal(t, s.Phase, types.TaskPhaseSucceeded)
	})

	t.Run("retryable-error", func(t *testing.T) {
		params := createExecutorInitializationParams(t, nil)
		store := params.DataStore
		msg := &core.ErrorDocument{
			Error: &core.ContainerError{
				Kind:    core.ContainerError_RECOVERABLE,
				Code:    "x",
				Message: "y",
			},
		}
		assert.NoError(t, store.WriteProtobuf(ctx, tctx.GetErrorFile(), storage.Options{}, msg))
		assert.NoError(t, k.Initialize(ctx, params))
		s, err := k.HandleTaskSuccess(ctx, tctx)
		assert.NoError(t, err)
		assert.Equal(t, s.Phase, types.TaskPhaseRetryableFailure)
		c, ok := taskerrs.GetErrorCode(s.Err)
		assert.True(t, ok)
		assert.Equal(t, c, "x")
	})

	t.Run("nonretryable-error", func(t *testing.T) {
		params := createExecutorInitializationParams(t, nil)
		store := params.DataStore
		msg := &core.ErrorDocument{
			Error: &core.ContainerError{
				Kind:    core.ContainerError_NON_RECOVERABLE,
				Code:    "m",
				Message: "n",
			},
		}
		assert.NoError(t, store.WriteProtobuf(ctx, tctx.GetErrorFile(), storage.Options{}, msg))
		assert.NoError(t, k.Initialize(ctx, params))
		s, err := k.HandleTaskSuccess(ctx, tctx)
		assert.NoError(t, err)
		assert.Equal(t, s.Phase, types.TaskPhasePermanentFailure)
		c, ok := taskerrs.GetErrorCode(s.Err)
		assert.True(t, ok)
		assert.Equal(t, c, "m")
	})

	t.Run("corrupted", func(t *testing.T) {
		params := createExecutorInitializationParams(t, nil)
		store := params.DataStore
		r := bytes.NewReader([]byte{'x'})
		assert.NoError(t, store.WriteRaw(ctx, tctx.GetErrorFile(), r.Size(), storage.Options{}, r))
		assert.NoError(t, k.Initialize(ctx, params))
		s, err := k.HandleTaskSuccess(ctx, tctx)
		assert.Error(t, err)
		assert.Equal(t, s.Phase, types.TaskPhaseUndefined)
	})
}

func assertObjectAndTaskCtx(t *testing.T, taskCtx types.TaskContext, resource k8s.Resource) {

}

func TestAddObjectMetadata(t *testing.T) {
	o := v1.Pod{}
	flytek8s.AddObjectMetadata()
	assert.Equal(t, taskCtx.GetTaskExecutionID().GetGeneratedName(), o.GetName())
	assert.Equal(t, []v12.OwnerReference{taskCtx.GetOwnerReference()}, o.GetOwnerReferences())
	assert.Equal(t, taskCtx.GetNamespace(), o.GetNamespace())
	assert.Equal(t, map[string]string{
		"cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
		"aKey": "aVal",
	}, o.GetAnnotations())
	assert.Equal(t, taskCtx.GetLabels(), o.GetLabels())
}

*/
