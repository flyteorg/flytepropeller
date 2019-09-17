package task

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/event"
	pluginMachinery "github.com/lyft/flyteplugins/go/tasks/pluginmachinery"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	pluginK8s "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/k8s"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/lyft/flytestdlib/storage"
	regErrors "github.com/pkg/errors"

	catalog2 "github.com/lyft/flytepropeller/pkg/controller/catalog"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/catalog"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/config"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/k8s"
)

const pluginContextKey = contextutils.Key("plugin")

type metrics struct {
	pluginPanics             labeled.Counter
	unsupportedTaskType      labeled.Counter
	discoveryPutFailureCount labeled.Counter
	discoveryGetFailureCount labeled.Counter
	discoveryMissCount       labeled.Counter
	discoveryHitCount        labeled.Counter

	// TODO We should have a metric to capture custom state size
}

type pluginRequestedTransition struct {
	ttype              handler.TransitionType
	pInfo              pluginCore.PhaseInfo
	previouslyObserved bool
	execInfo           handler.ExecutionInfo
}

func (p *pluginRequestedTransition) CacheHit() {
	p.ttype = handler.TransitionTypeEphemeral
	p.pInfo = pluginCore.PhaseInfoSuccess(nil)
	if p.execInfo.TaskNodeInfo == nil {
		p.execInfo.TaskNodeInfo = &handler.TaskNodeInfo{}
	}
	p.execInfo.TaskNodeInfo.CacheHit = true
}

func (p *pluginRequestedTransition) ObservedTransition(trns pluginCore.Transition) {
	p.ttype = ToTransitionType(trns.Type())
	p.pInfo = trns.Info()
}

func (p *pluginRequestedTransition) ObservedExecutionError(executionError *io.ExecutionError) {
	if executionError.IsRecoverable {
		p.pInfo = pluginCore.PhaseInfoFailed(pluginCore.PhaseRetryableFailure, executionError.ExecutionError, p.pInfo.Info())
	} else {
		p.pInfo = pluginCore.PhaseInfoFailed(pluginCore.PhasePermanentFailure, executionError.ExecutionError, p.pInfo.Info())
	}
}

func (p *pluginRequestedTransition) TransitionPreviouslyRecorded() {
	p.previouslyObserved = true
}

func (p *pluginRequestedTransition) FinalTaskEvent(id *core.TaskExecutionIdentifier, in io.InputFilePaths, out io.OutputFilePaths) (*event.TaskExecutionEvent, error) {
	if p.previouslyObserved {
		return nil, nil
	}
	return ToTaskExecutionEvent(id, in, out, p.pInfo)
}

func (p *pluginRequestedTransition) ObserveSuccess(outputPath storage.DataReference) {
	p.execInfo.OutputInfo = &handler.OutputInfo{OutputURI: outputPath}
}

func (p *pluginRequestedTransition) FinalTransition(ctx context.Context) (handler.Transition, error) {
	switch p.pInfo.Phase() {
	case pluginCore.PhaseSuccess:
		logger.Debugf(ctx, "Transitioning to Success")
		return handler.DoTransition(p.ttype, handler.PhaseInfoSuccess(&p.execInfo)), nil
	case pluginCore.PhaseRetryableFailure:
		logger.Debugf(ctx, "Transitioning to RetryableFailure")
		return handler.DoTransition(p.ttype, handler.PhaseInfoRetryableFailureErr(p.pInfo.Err(), nil)), nil
	case pluginCore.PhasePermanentFailure:
		logger.Debugf(ctx, "Transitioning to Failure")
		return handler.DoTransition(p.ttype, handler.PhaseInfoFailureErr(p.pInfo.Err(), nil)), nil
	case pluginCore.PhaseUndefined:
		return handler.UnknownTransition, fmt.Errorf("error converting plugin phase, received [Undefined]")
	}

	logger.Debugf(ctx, "Task still running")
	return handler.DoTransition(p.ttype, handler.PhaseInfoRunning(nil)), nil
}

// The plugin interface available especially for testing.
type PluginRegistryIface interface {
	GetCorePlugins() []pluginCore.PluginEntry
	GetK8sPlugins() []pluginK8s.PluginEntry
}

type Handler struct {
	catalog        catalog.Client
	plugins        map[pluginCore.TaskType]pluginCore.Plugin
	defaultPlugin  pluginCore.Plugin
	metrics        *metrics
	pluginRegistry PluginRegistryIface
	kubeClient     pluginCore.KubeClient
	cfg            *config.Config
}

func (t *Handler) FinalizeRequired() bool {
	return true
}

func (t *Handler) setDefault(ctx context.Context, p pluginCore.Plugin) error {
	if t.defaultPlugin != nil {
		logger.Errorf(ctx, "cannot set plugin [%s] as default as plugin [%s] is already configured as default", p.GetID(), t.defaultPlugin.GetID())
	} else {
		logger.Infof(ctx, "Plugin [%s] registered as default plugin", p.GetID())
		t.defaultPlugin = p
	}
	return nil
}

func (t *Handler) Setup(ctx context.Context, sCtx handler.SetupContext) error {
	tSCtx := &setupContext{
		SetupContext: sCtx,
		kubeClient:   t.kubeClient,
	}

	enabledPlugins := t.cfg.TaskPlugins.GetEnabledPluginsSet()
	allPluginsEnabled := false
	if enabledPlugins.Len() == 0 {
		allPluginsEnabled = true
	}

	logger.Infof(ctx, "Loading core Plugins, plugin configuration [all plugins enabled: %s]", allPluginsEnabled)
	for _, cpe := range t.pluginRegistry.GetCorePlugins() {
		if !allPluginsEnabled && enabledPlugins.Has(cpe.ID) {
			logger.Infof(ctx, "Plugin [%s] is DISABLED.", cpe.ID)
		} else {
			logger.Infof(ctx, "Loading Plugin [%s] ENABLED", cpe.ID)
			cp, err := cpe.LoadPlugin(ctx, tSCtx)
			if err != nil {
				return regErrors.Wrapf(err, "failed to load plugin - %s", cpe.ID)
			}
			for _, tt := range cpe.RegisteredTaskTypes {
				logger.Infof(ctx, "Plugin [%s] registered for TaskType [%s]", cpe.ID, tt)
				t.plugins[tt] = cp
			}
			if cpe.IsDefault {
				if err := t.setDefault(ctx, cp); err != nil {
					return err
				}
			}
		}
	}
	for _, kpe := range t.pluginRegistry.GetK8sPlugins() {
		if !allPluginsEnabled && enabledPlugins.Has(kpe.ID) {
			logger.Infof(ctx, "K8s Plugin [%s] is DISABLED.", kpe.ID)
		} else {
			kp, err := k8s.NewPluginManager(ctx, tSCtx, kpe)
			if err != nil {
				return regErrors.Wrapf(err, "failed to load plugin - %s", kpe.ID)
			}
			for _, tt := range kpe.RegisteredTaskTypes {
				t.plugins[tt] = kp
			}
			if kpe.IsDefault {
				if err := t.setDefault(ctx, kp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (t Handler) ResolvePlugin(ctx context.Context, ttype string) (pluginCore.Plugin, error) {
	p, ok := t.plugins[ttype]
	if ok {
		logger.Debugf(ctx, "Plugin [%s] resolved for Handler type [%s]", p.GetID(), ttype)
		return p, nil
	}
	if t.defaultPlugin != nil {
		logger.Warnf(ctx, "No plugin found for Handler-type [%s], defaulting to [%s]", ttype, t.defaultPlugin.GetID())
		return t.defaultPlugin, nil
	}
	return nil, fmt.Errorf("no plugin defined for Handler type [%s] and no defaultPlugin configured", ttype)
}

func (t Handler) invokePlugin(ctx context.Context, p pluginCore.Plugin, tCtx *taskExecutionContext, ts handler.TaskNodeState) (*pluginRequestedTransition, error) {
	trns, err := func() (trns pluginCore.Transition, err error) {
		defer func() {
			if r := recover(); r != nil {
				t.metrics.pluginPanics.Inc(ctx)
				stack := debug.Stack()
				logger.Errorf(ctx, "Panic in plugin[%s]", p.GetID())
				err = fmt.Errorf("panic when executing a plugin [%s]. Stack: [%s]", p.GetID(), string(stack))
				trns = pluginCore.UnknownTransition
			}
		}()
		childCtx := context.WithValue(ctx, pluginContextKey, p.GetID())
		trns, err = p.Handle(childCtx, tCtx)
		return
	}()
	if err != nil {
		logger.Warnf(ctx, "Runtime error from plugin [%s]. Error: %s", p.GetID(), err.Error())
		return nil, regErrors.Wrapf(err, "failed to execute handle for plugin [%s]", p.GetID())
	}

	pluginTrns := &pluginRequestedTransition{}
	pluginTrns.ObservedTransition(trns)

	if trns.Info().Phase() == ts.PluginPhase {
		if trns.Info().Version() == ts.PluginPhaseVersion {
			logger.Debugf(ctx, "p+Version previously seen .. no event will be sent")
			pluginTrns.TransitionPreviouslyRecorded()
			return pluginTrns, nil
		}
		if trns.Info().Version() > uint32(t.cfg.MaxPluginPhaseVersions) {
			logger.Errorf(ctx, "Too many Plugin p versions for plugin [%s]. p versions [%d/%d]", p.GetID(), trns.Info().Version(), t.cfg.MaxPluginPhaseVersions)
			pluginTrns.ObservedExecutionError(&io.ExecutionError{
				ExecutionError: &core.ExecutionError{
					Code:    "TooManyPluginPhaseVersions",
					Message: fmt.Sprintf("Total number of phase versions exceeded for p [%s] in Plugin [%s], max allowed [%d]", trns.Info().Phase().String(), p.GetID(), t.cfg.MaxPluginPhaseVersions),
				},
				IsRecoverable: false,
			})
			return pluginTrns, nil
		}
	}

	if trns.Info().Phase() == pluginCore.PhaseSuccess {
		// -------------------------------------
		// TODO: @kumare create Issue# Remove the code after we use closures to handle dynamic nodes
		// This code only exists to support Dynamic tasks. Eventually dynamic tasks will use closure nodes to execute
		// Until then we have to check if the Handler executed resulted in a dynamic node being generated, if so, then
		// we will not check for outputs or call onTaskSuccess. The reason is that outputs have not yet been materialized.
		// Outputs for the parent node will only get generated after the subtasks complete. We have to wait for the completion
		// the dynamic.handler will call onTaskSuccess for the parent node

		f, err := NewRemoteFutureFileReader(ctx, tCtx.ow.dataDir, tCtx.DataStore())
		if err != nil {
			return nil, regErrors.Wrapf(err, "failed to create remote file reader")
		}
		if ok, err := f.Exists(ctx); err != nil {
			logger.Errorf(ctx, "failed to check existence of futures file")
			return nil, regErrors.Wrapf(err, "failed to check existence of futures file")
		} else if ok {
			logger.Infof(ctx, "Futures file exists, this is a dynamic parent-Handler will not run onTaskSuccess")
			return pluginTrns, nil
		}
		// End TODO
		// -------------------------------------
		logger.Debugf(ctx, "Task success detected, calling on Task success")
		ee, err := t.ValidateOutputAndCacheAdd(ctx, tCtx.InputReader(), tCtx.ow.outReader, tCtx.tr, catalog.Metadata{
			WorkflowExecutionIdentifier: nil,
			TaskExecutionIdentifier: &core.TaskExecutionIdentifier{
				TaskId: tCtx.tr.GetTaskID(),
				// TODO @kumare required before checking in
				NodeExecutionId: nil,
				RetryAttempt:    0,
			},
		})
		if err != nil {
			return nil, err
		}
		if ee != nil {
			pluginTrns.ObservedExecutionError(ee)
		} else {
			pluginTrns.ObserveSuccess(tCtx.ow.outPath)
		}
	}
	return pluginTrns, nil
}

func (t Handler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {

	tCtx, err := t.newTaskExecutionContext(ctx, nCtx)
	if err != nil {
		return handler.UnknownTransition, errors.Wrapf(errors.IllegalStateError, nCtx.NodeID(), err, "unable to create Handler execution context")
	}

	ttype := tCtx.tr.GetTaskType()
	ctx = contextutils.WithTaskType(ctx, ttype)
	p, err := t.ResolvePlugin(ctx, ttype)
	if err != nil {
		return handler.UnknownTransition, errors.Wrapf(errors.UnsupportedTaskTypeError, nCtx.NodeID(), err, "unable to resolve plugin")
	}

	ts := nCtx.NodeStateReader().GetTaskNodeState()

	var pluginTrns *pluginRequestedTransition

	// NOTE: Ideally we should use a taskExecution state for this handler. But, doing that will make it completely backwards incompatible
	// So now we will derive this from the plugin phase
	// TODO @kumare re-evaluate this decision

	// STEP 1: Check Cache
	if ts.PluginPhase == pluginCore.PhaseUndefined {
		// This is assumed to be first time. we will check catalog and call handle
		if ok, err := t.CheckCatalogCache(ctx, tCtx.tr, nCtx.InputReader(), tCtx.ow); err != nil {
			logger.Errorf(ctx, "failed to check catalog cache with error")
			return handler.UnknownTransition, err
		} else if ok {
			r := tCtx.ow.GetReader()
			if r != nil {
				// TODO @kumare this can be optimized, if we have paths then the reader could be pipelined to a sink
				o, ee, err := r.Read(ctx)
				if err != nil {
					logger.Errorf(ctx, "failed to read from catalog, err: %s", err.Error())
					return handler.UnknownTransition, err
				}
				if ee != nil {
					logger.Errorf(ctx, "got execution error from catalog output reader? This should not happen, err: %s", ee.String())
					return handler.UnknownTransition, errors.Errorf(errors.IllegalStateError, nCtx.NodeID(), "execution error from a cache output, bad state: %s", ee.String())
				}
				if err := nCtx.DataStore().WriteProtobuf(ctx, tCtx.ow.outPath, storage.Options{}, o); err != nil {
					logger.Errorf(ctx, "failed to write cached value to datastore, err: %s", err.Error())
					return handler.UnknownTransition, err
				}
				pluginTrns = &pluginRequestedTransition{}
				pluginTrns.CacheHit()
			} else {
				logger.Errorf(ctx, "no output reader found after a catalog cache hit!")
			}
		}
	}

	// STEP 2: If no cache-hit, then lets invoke the plugin and wait for a transition out of undefined
	if pluginTrns == nil {
		var err error
		pluginTrns, err = t.invokePlugin(ctx, p, tCtx, ts)
		if err != nil {
			return handler.UnknownTransition, errors.Wrapf(errors.RuntimeExecutionError, nCtx.NodeID(), err, "failed during plugin execution")
		}
	}

	// STEP 3: Sanity check
	if pluginTrns == nil {
		// Still nil, this should never happen!!!
		return handler.UnknownTransition, errors.Errorf(errors.IllegalStateError, nCtx.NodeID(), "plugin transition is not observed and no error as well.")
	}

	execID := tCtx.TaskExecutionMetadata().GetTaskExecutionID().GetID()
	// STEP 4: Send buffered events!
	logger.Debugf(ctx, "Sending buffered Task events.")
	for _, ev := range tCtx.ber.GetAll(ctx) {
		evInfo, err := ToTaskExecutionEvent(&execID, nCtx.InputReader(), tCtx.ow, ev)
		if err != nil {
			return handler.UnknownTransition, err
		}
		if err := nCtx.EventsRecorder().RecordTaskEvent(ctx, evInfo); err != nil {
			logger.Errorf(ctx, "Event recording failed for Plugin [%s], eventPhase [%s], error :%s", p.GetID(), evInfo.Phase.String(), err.Error())
			// Check for idempotency
			// Check for terminate state error
			return handler.UnknownTransition, err
		}
	}

	// STEP 5: Send Transition events
	logger.Debugf(ctx, "Sending transition event for plugin phase [%s]", pluginTrns.pInfo.Phase().String())
	evInfo, err := pluginTrns.FinalTaskEvent(&execID, nCtx.InputReader(), tCtx.ow)
	if err != nil {
		logger.Errorf(ctx, "failed to convert plugin transition to TaskExecutionEvent. Error: %s", err.Error())
		return handler.UnknownTransition, err
	}
	if evInfo != nil {
		if err := nCtx.EventsRecorder().RecordTaskEvent(ctx, evInfo); err != nil {
			// Check for idempotency
			// Check for terminate state error
			logger.Errorf(ctx, "failed to send event to Admin. error: %s", err.Error())
			return handler.UnknownTransition, err
		}
	} else {
		logger.Debugf(ctx, "Received no event to record.")
	}

	// STEP 6: Persist the plugin state
	var b []byte
	var v uint32
	if tCtx.psm.newState != nil {
		b = tCtx.psm.newState.Bytes()
		v = uint32(tCtx.psm.newStateVersion)
	} else {
		// New state was not mutated, so we should write back the existing state
		b = ts.PluginState
		v = ts.PluginPhaseVersion
	}
	err = nCtx.NodeStateWriter().PutTaskNodeState(handler.TaskNodeState{
		PluginState:        b,
		PluginStateVersion: v,
		PluginPhase:        pluginTrns.pInfo.Phase(),
		PluginPhaseVersion: pluginTrns.pInfo.Version(),
	})
	if err != nil {
		logger.Errorf(ctx, "Failed to store TaskNode state, err :%s", err.Error())
		return handler.UnknownTransition, err
	}

	return pluginTrns.FinalTransition(ctx)
}

func (t Handler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext) error {
	logger.Debugf(ctx, "Abort invoked.")
	tCtx, err := t.newTaskExecutionContext(ctx, nCtx)
	if err != nil {
		return errors.Wrapf(errors.IllegalStateError, nCtx.NodeID(), err, "unable to create Handler execution context")
	}

	p, err := t.ResolvePlugin(ctx, tCtx.tr.GetTaskType())
	if err != nil {
		return errors.Wrapf(errors.UnsupportedTaskTypeError, nCtx.NodeID(), err, "unable to resolve plugin")
	}

	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.metrics.pluginPanics.Inc(ctx)
				stack := debug.Stack()
				logger.Errorf(ctx, "Panic in plugin.Abort for TaskType [%s]", tCtx.tr.GetTaskType())
				err = fmt.Errorf("panic when executing a plugin for TaskType [%s]. Stack: [%s]", tCtx.tr.GetTaskType(), string(stack))
			}
		}()
		childCtx := context.WithValue(ctx, pluginContextKey, p.GetID())
		err = p.Abort(childCtx, tCtx)
		return
	}()
}

func (t Handler) Finalize(ctx context.Context, nCtx handler.NodeExecutionContext) error {
	logger.Debugf(ctx, "Finalize invoked.")
	tCtx, err := t.newTaskExecutionContext(ctx, nCtx)
	if err != nil {
		return errors.Wrapf(errors.IllegalStateError, nCtx.NodeID(), err, "unable to create Handler execution context")
	}

	p, err := t.ResolvePlugin(ctx, tCtx.tr.GetTaskType())
	if err != nil {
		return errors.Wrapf(errors.UnsupportedTaskTypeError, nCtx.NodeID(), err, "unable to resolve plugin")
	}

	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				t.metrics.pluginPanics.Inc(ctx)
				stack := debug.Stack()
				logger.Errorf(ctx, "Panic in plugin.Abort for TaskType [%s]", tCtx.tr.GetTaskType())
				err = fmt.Errorf("panic when executing a plugin for TaskType [%s]. Stack: [%s]", tCtx.tr.GetTaskType(), string(stack))
			}
		}()
		childCtx := context.WithValue(ctx, pluginContextKey, p.GetID())
		err = p.Finalize(childCtx, tCtx)
		return
	}()
}

func New(_ context.Context, kubeClient executors.Client, client catalog2.Client, scope promutils.Scope) *Handler {

	return &Handler{
		pluginRegistry: pluginMachinery.PluginRegistry(),
		plugins:        make(map[pluginCore.TaskType]pluginCore.Plugin),
		metrics: &metrics{
			pluginPanics:             labeled.NewCounter("plugin_panic", "Task plugin paniced when trying to execute a Handler.", scope),
			unsupportedTaskType:      labeled.NewCounter("unsupported_tasktype", "No Handler plugin configured for Handler type", scope),
			discoveryHitCount:        labeled.NewCounter("discovery_hit_count", "Task cached in Discovery", scope),
			discoveryMissCount:       labeled.NewCounter("discovery_miss_count", "Task not cached in Discovery", scope),
			discoveryPutFailureCount: labeled.NewCounter("discovery_put_failure_count", "Discovery Put failure count", scope),
			discoveryGetFailureCount: labeled.NewCounter("discovery_get_failure_count", "Discovery Get faillure count", scope),
		},
		kubeClient: kubeClient,
		catalog:    catalog.NOOPCatalog{},
		cfg:        config.GetConfig(),
	}
}
