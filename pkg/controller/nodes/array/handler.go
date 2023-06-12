package array

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/event"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/flyteorg/flyteplugins/go/tasks/plugins/array/errorcollector"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/validators"
	"github.com/flyteorg/flytepropeller/pkg/controller/config"
	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/errors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/codex"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/k8s"

	"github.com/flyteorg/flytestdlib/bitarray"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/storage"

	"github.com/golang/protobuf/ptypes"
)

var (
	nilLiteral = &idlcore.Literal{
		Value: &idlcore.Literal_Scalar{
			Scalar: &idlcore.Scalar{
				Value: &idlcore.Scalar_NoneType{
					NoneType: &idlcore.Void{},
				},
			},
		},
	}
)

//go:generate mockery -all -case=underscore

// arrayNodeHandler is a handle implementation for processing array nodes
type arrayNodeHandler struct {
	metrics                    metrics
	nodeExecutor               interfaces.Node
	pluginStateBytesNotStarted []byte
	pluginStateBytesStarted    []byte
}

// metrics encapsulates the prometheus metrics for this handler
type metrics struct {
	scope promutils.Scope
}

// newMetrics initializes a new metrics struct
func newMetrics(scope promutils.Scope) metrics {
	return metrics{
		scope: scope,
	}
}

// Abort stops the array node defined in the NodeExecutionContext
func (a *arrayNodeHandler) Abort(ctx context.Context, nCtx interfaces.NodeExecutionContext, reason string) error {
	arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

	externalResources := make([]*event.ExternalResourceInfo, 0, len(arrayNodeState.SubNodePhases.GetItems()))
	messageCollector := errorcollector.NewErrorMessageCollector()
	switch arrayNodeState.Phase {
	case v1alpha1.ArrayNodePhaseExecuting, v1alpha1.ArrayNodePhaseFailing:
		currentParallelism := uint32(0)
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)

			// TODO @hamersaw fix - do not process nodes that haven't started or are in a terminal state
			//if nodes.IsNotyetStarted(nodePhase) || nodes.IsTerminalNodePhase(nodePhase) {
			if nodePhase == v1alpha1.NodePhaseNotYetStarted || nodePhase == v1alpha1.NodePhaseSucceeded || nodePhase == v1alpha1.NodePhaseFailed || nodePhase == v1alpha1.NodePhaseTimedOut || nodePhase == v1alpha1.NodePhaseSkipped || nodePhase == v1alpha1.NodePhaseRecovered {
				continue
			}

			// create array contexts
			arrayNodeExecutor, arrayExecutionContext, arrayDAGStructure, arrayNodeLookup, subNodeSpec, _, _, err :=
				a.buildArrayNodeContext(ctx, nCtx, &arrayNodeState, arrayNode, i, &currentParallelism)

			// abort subNode
			err = arrayNodeExecutor.AbortHandler(ctx, arrayExecutionContext, arrayDAGStructure, arrayNodeLookup, subNodeSpec, reason)
			if err != nil {
				messageCollector.Collect(i, err.Error())
			} else {
				externalResources = append(externalResources, &event.ExternalResourceInfo{
					ExternalId:   fmt.Sprintf("%s-%d", nCtx.NodeID, i), // TODO @hamersaw do better
					Index:        uint32(i),
					Logs:         nil,
					RetryAttempt: 0,
					Phase:        idlcore.TaskExecution_ABORTED,
				})
			}
		}
	}

	if messageCollector.Length() > 0 {
		return fmt.Errorf(messageCollector.Summary(512)) // TODO @hamersaw - make configurable
	}

	// TODO @hamersaw - update aborted state for subnodes
	taskExecutionEvent, err := buildTaskExecutionEvent(ctx, nCtx, idlcore.TaskExecution_ABORTED, 0, externalResources)
	if err != nil {
		return err
	}

	// TODO @hamersaw - pass eventConfig correctly
	if err := nCtx.EventsRecorder().RecordTaskEvent(ctx, taskExecutionEvent, &config.EventConfig{}); err != nil {
		logger.Errorf(ctx, "ArrayNode event recording failed: [%s]", err.Error())
		return err
	}

	return nil
}

// Finalize completes the array node defined in the NodeExecutionContext
func (a *arrayNodeHandler) Finalize(ctx context.Context, nCtx interfaces.NodeExecutionContext) error {
	arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

	messageCollector := errorcollector.NewErrorMessageCollector()
	switch arrayNodeState.Phase {
	case v1alpha1.ArrayNodePhaseExecuting, v1alpha1.ArrayNodePhaseFailing, v1alpha1.ArrayNodePhaseSucceeding:
		currentParallelism := uint32(0)
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)

			// TODO @hamersaw fix - do not process nodes that haven't started
			//if nodes.IsNotyetStarted(nodePhase) {
			if nodePhase == v1alpha1.NodePhaseNotYetStarted {
				continue
			}

			// create array contexts
			arrayNodeExecutor, arrayExecutionContext, arrayDAGStructure, arrayNodeLookup, subNodeSpec, _, _, err :=
				a.buildArrayNodeContext(ctx, nCtx, &arrayNodeState, arrayNode, i, &currentParallelism)

			// finalize subNode
			err = arrayNodeExecutor.FinalizeHandler(ctx, arrayExecutionContext, arrayDAGStructure, arrayNodeLookup, subNodeSpec)
			if err != nil {
				messageCollector.Collect(i, err.Error())
			}
		}
	}

	if messageCollector.Length() > 0 {
		return fmt.Errorf(messageCollector.Summary(512)) // TODO @hamersaw - make configurable
	}

	return nil
}

// FinalizeRequired defines whether or not this handler requires finalize to be called on node
// completion
func (a *arrayNodeHandler) FinalizeRequired() bool {
	 // must return true because we can't determine if finalize is required for the subNode
	return true
}

// Handle is responsible for transitioning and reporting node state to complete the node defined
// by the NodeExecutionContext
func (a *arrayNodeHandler) Handle(ctx context.Context, nCtx interfaces.NodeExecutionContext) (handler.Transition, error) {
	arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

	var externalResources []*event.ExternalResourceInfo
	taskPhaseVersion := arrayNodeState.TaskPhaseVersion

	switch arrayNodeState.Phase {
	case v1alpha1.ArrayNodePhaseNone:
		// identify and validate array node input value lengths
		literalMap, err := nCtx.InputReader().Get(ctx)
		if err != nil {
			return handler.UnknownTransition, err
		}

		size := -1
		for _, variable := range literalMap.Literals {
			literalType := validators.LiteralTypeForLiteral(variable)
			switch literalType.Type.(type) {
			case *idlcore.LiteralType_CollectionType:
				collectionLength := len(variable.GetCollection().Literals)

				if size == -1 {
					size = collectionLength
				} else if size != collectionLength {
					return handler.DoTransition(handler.TransitionTypeEphemeral,
						handler.PhaseInfoFailure(idlcore.ExecutionError_USER, errors.InvalidArrayLength,
							fmt.Sprintf("input arrays have different lengths: expecting '%d' found '%d'", size, collectionLength), nil),
					), nil
				}
			}
		}

		if size == -1 {
			return handler.DoTransition(handler.TransitionTypeEphemeral,
				handler.PhaseInfoFailure(idlcore.ExecutionError_USER, errors.InvalidArrayLength, "no input array provided", nil),
			), nil
		}

		// initialize ArrayNode state
		maxAttempts := task.DefaultMaxAttempts
		subNodeSpec := *arrayNode.GetSubNodeSpec()
		if subNodeSpec.GetRetryStrategy() != nil && subNodeSpec.GetRetryStrategy().MinAttempts != nil {
			maxAttempts = *subNodeSpec.GetRetryStrategy().MinAttempts
		}

		for _, item := range []struct{arrayReference *bitarray.CompactArray; maxValue int}{
				{arrayReference: &arrayNodeState.SubNodePhases, maxValue: len(core.Phases)-1}, // TODO @hamersaw - maxValue is for task phases
				{arrayReference: &arrayNodeState.SubNodeTaskPhases, maxValue: len(core.Phases)-1},
				{arrayReference: &arrayNodeState.SubNodeRetryAttempts, maxValue: maxAttempts},
				{arrayReference: &arrayNodeState.SubNodeSystemFailures, maxValue: maxAttempts},
			} {

			*item.arrayReference, err = bitarray.NewCompactArray(uint(size), bitarray.Item(item.maxValue))
			if err != nil {
				return handler.UnknownTransition, err
			}
		}

		// initialize externalResources
		externalResources = make([]*event.ExternalResourceInfo, 0, size)
		for i := 0; i < size; i++ {
			externalResources = append(externalResources, &event.ExternalResourceInfo{
				ExternalId:   fmt.Sprintf("%s-%d", nCtx.NodeID, i), // TODO @hamersaw do better
				Index:        uint32(i),
				Logs:         nil,
				RetryAttempt: 0,
				Phase:        idlcore.TaskExecution_QUEUED,
			})
		}

		// transition ArrayNode to `ArrayNodePhaseExecuting`
		arrayNodeState.Phase = v1alpha1.ArrayNodePhaseExecuting
	case v1alpha1.ArrayNodePhaseExecuting:
		// process array node subnodes
		currentParallelism := uint32(0)
		messageCollector := errorcollector.NewErrorMessageCollector()
		externalResources = make([]*event.ExternalResourceInfo, 0)
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)

			// TODO @hamersaw fix - do not process nodes in terminal state
			//if nodes.IsTerminalNodePhase(nodePhase) {
			if nodePhase == v1alpha1.NodePhaseSucceeded || nodePhase == v1alpha1.NodePhaseFailed || nodePhase == v1alpha1.NodePhaseTimedOut || nodePhase == v1alpha1.NodePhaseSkipped || nodePhase == v1alpha1.NodePhaseRecovered {
				continue
			}

			// create array contexts
			arrayNodeExecutor, arrayExecutionContext, arrayDAGStructure, arrayNodeLookup, subNodeSpec, subNodeStatus, arrayEventRecorder, err :=
				a.buildArrayNodeContext(ctx, nCtx, &arrayNodeState, arrayNode, i, &currentParallelism)

			// execute subNode
			_, err = arrayNodeExecutor.RecursiveNodeHandler(ctx, arrayExecutionContext, arrayDAGStructure, arrayNodeLookup, subNodeSpec)
			if err != nil {
				return handler.UnknownTransition, err
			}

			// capture subNode error if exists
			if subNodeStatus.Error != nil {
				messageCollector.Collect(i, subNodeStatus.Error.Message)
			}

			// process events
			cacheStatus := idlcore.CatalogCacheStatus_CACHE_DISABLED
			for _, nodeExecutionEvent := range arrayEventRecorder.NodeEvents() {
				switch target := nodeExecutionEvent.TargetMetadata.(type) {
				case *event.NodeExecutionEvent_TaskNodeMetadata:
					if target.TaskNodeMetadata != nil {
						cacheStatus = target.TaskNodeMetadata.CacheStatus
					}
				}
			}

			for _, taskExecutionEvent := range arrayEventRecorder.TaskEvents() {
				taskPhase := idlcore.TaskExecution_UNDEFINED
				if taskNodeStatus := subNodeStatus.GetTaskNodeStatus(); taskNodeStatus != nil {
					taskPhase = task.ToTaskEventPhase(core.Phase(taskNodeStatus.GetPhase()))
				}

				for _, log := range taskExecutionEvent.Logs {
					// TODO @hamersaw - do we need to add retryattempt?
					log.Name = fmt.Sprintf("%s-%d", log.Name, i)
				}

				externalResources = append(externalResources, &event.ExternalResourceInfo{
					ExternalId:   fmt.Sprintf("%s-%d", nCtx.NodeID, i), // TODO @hamersaw do better
					Index:        uint32(i),
					Logs:         taskExecutionEvent.Logs,
					RetryAttempt: 0,
					Phase:        taskPhase,
					CacheStatus:  cacheStatus,
				})
			}

			//fmt.Printf("HAMERSAW - '%d' transition node phase %d -> %d task phase '%d' -> '%d'\n", i,
			//	nodePhase, subNodeStatus.GetPhase(), taskPhase, subNodeStatus.GetTaskNodeStatus().GetPhase())

			// update subNode state
			arrayNodeState.SubNodePhases.SetItem(i, uint64(subNodeStatus.GetPhase()))
			if subNodeStatus.GetTaskNodeStatus() == nil {
				// TODO @hamersaw during retries we clear the GetTaskNodeStatus - so resetting task phase
				arrayNodeState.SubNodeTaskPhases.SetItem(i, uint64(0))
			} else {
				arrayNodeState.SubNodeTaskPhases.SetItem(i, uint64(subNodeStatus.GetTaskNodeStatus().GetPhase()))
			}
			arrayNodeState.SubNodeRetryAttempts.SetItem(i, uint64(subNodeStatus.GetAttempts()))
			arrayNodeState.SubNodeSystemFailures.SetItem(i, uint64(subNodeStatus.GetSystemFailures()))
		}

		// process phases of subNodes to determine overall `ArrayNode` phase
		successCount := 0
		failedCount := 0
		failingCount := 0
		runningCount := 0
		for _, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)
			//fmt.Printf("HAMERSAW - node %d phase %d\n", i, nodePhase)
			switch nodePhase {
			case v1alpha1.NodePhaseSucceeded, v1alpha1.NodePhaseRecovered: // TODO @hamersaw NodePhaseSkipped?
				successCount++
			case v1alpha1.NodePhaseFailing:
				failingCount++
	 		case v1alpha1.NodePhaseFailed: // TODO @hamersaw NodePhaseTimedOut?
				failedCount++
			default:
				runningCount++
			}
		}

		// calculate minimum number of successes to succeed the ArrayNode
		minSuccesses := len(arrayNodeState.SubNodePhases.GetItems())
		if arrayNode.GetMinSuccesses() != nil {
			minSuccesses = int(*arrayNode.GetMinSuccesses())
		} else if minSuccessRatio := arrayNode.GetMinSuccessRatio(); minSuccessRatio != nil {
			minSuccesses = int(math.Ceil(float64(*minSuccessRatio) * float64(minSuccesses)))
		}

		// if there is a failing node set the error message
		if failingCount > 0 && arrayNodeState.Error == nil {
			arrayNodeState.Error = &idlcore.ExecutionError{
				Message: messageCollector.Summary(512), // TODO @hamersaw - make configurable
			}
		}

		if len(arrayNodeState.SubNodePhases.GetItems()) - failedCount < minSuccesses {
			// no chance to reach the mininum number of successes
			arrayNodeState.Phase = v1alpha1.ArrayNodePhaseFailing
		} else if successCount >= minSuccesses && runningCount == 0 {
			// wait until all tasks have completed before declaring success
			arrayNodeState.Phase = v1alpha1.ArrayNodePhaseSucceeding
		}
	case v1alpha1.ArrayNodePhaseFailing:
		if err := a.Abort(ctx, nCtx, "ArrayNodeFailing"); err != nil {
			return handler.UnknownTransition, err
		}

		// fail with reported error if one exists
		if arrayNodeState.Error != nil {
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailureErr(arrayNodeState.Error, nil)), nil
		}

		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(
			idlcore.ExecutionError_UNKNOWN,
			"ArrayNodeFailing",
			"Unknown reason",
			nil,
		)), nil
	case v1alpha1.ArrayNodePhaseSucceeding:
		outputLiterals := make(map[string]*idlcore.Literal)
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)

			if nodePhase != v1alpha1.NodePhaseSucceeded {
				// retrieve output variables from task template
				var outputVariables map[string]*idlcore.Variable
				task, err := nCtx.ExecutionContext().GetTask(*arrayNode.GetSubNodeSpec().TaskRef)
				if err != nil {
					// Should never happen
					return handler.UnknownTransition, err
				}

				if task.CoreTask() != nil && task.CoreTask().Interface != nil && task.CoreTask().Interface.Outputs != nil {
					outputVariables = task.CoreTask().Interface.Outputs.Variables
				}

				// append nil literal for all ouput variables
				for name, _ := range outputVariables {
					appendLiteral(name, nilLiteral, outputLiterals, len(arrayNodeState.SubNodePhases.GetItems()))
				}
			} else {
				// initialize subNode reader
				currentAttempt := uint32(arrayNodeState.SubNodeRetryAttempts.GetItem(i))
				subDataDir, subOutputDir, err := constructOutputReferences(ctx, nCtx, strconv.Itoa(i), strconv.Itoa(int(currentAttempt)))
				if err != nil {
					return handler.UnknownTransition, err
				}

				// checkpoint paths are not computed here because this function is only called when writing
				// existing cached outputs. if this functionality changes this will need to be revisited.
				outputPaths := ioutils.NewCheckpointRemoteFilePaths(ctx, nCtx.DataStore(), subOutputDir, ioutils.NewRawOutputPaths(ctx, subDataDir), "")
				reader := ioutils.NewRemoteFileOutputReader(ctx, nCtx.DataStore(), outputPaths, int64(999999999))

				// read outputs
				outputs, executionErr, err := reader.Read(ctx)
				if err != nil {
					return handler.UnknownTransition, err
				} else if executionErr != nil {
					// TODO @hamersaw handle executionErr
					//return handler.UnknownTransition, executionErr
				}

				// copy individual subNode output literals into a collection of output literals
				for name, literal := range outputs.GetLiterals() {
					appendLiteral(name, literal, outputLiterals, len(arrayNodeState.SubNodePhases.GetItems()))
				}
			}
		}

		outputLiteralMap := &idlcore.LiteralMap{
			Literals: outputLiterals,
		}

		//fmt.Printf("HAMERSAW - final outputs %+v\n", outputLiteralMap)
		outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
		if err := nCtx.DataStore().WriteProtobuf(ctx, outputFile, storage.Options{}, outputLiteralMap); err != nil {
			return handler.UnknownTransition, err
		}

		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(
			&handler.ExecutionInfo{
				OutputInfo: &handler.OutputInfo{
					OutputURI: outputFile,
				},
			},
		)), nil
	default:
		// TODO @hamersaw - fail
	}

	// TODO @hamersaw - send task-level events - this requires externalResources to emulate current maptasks
	if len(externalResources) > 0 {
		/*occurredAt, err := ptypes.TimestampProto(time.Now())
		if err != nil {
			return handler.UnknownTransition, err
		}

		nodeExecutionId := nCtx.NodeExecutionMetadata().GetNodeExecutionID()
		workflowExecutionId := nodeExecutionId.ExecutionId
		taskExecutionEvent := &event.TaskExecutionEvent{
			TaskId: &idlcore.Identifier{
				ResourceType: idlcore.ResourceType_TASK,
				Project:      workflowExecutionId.Project,
				Domain:       workflowExecutionId.Domain,
				Name:         nCtx.NodeID(),
				Version:      "v1", // TODO @hamersaw - please
			},
			ParentNodeExecutionId: nCtx.NodeExecutionMetadata().GetNodeExecutionID(),
			RetryAttempt:          0, // ArrayNode will never retry 
			Phase:                 2, // TODO @hamersaw - determine node phase from ArrayNodePhase (ie. Queued, Running, Succeeded, Failed)
			PhaseVersion:          taskPhaseVersion,
			OccurredAt:            occurredAt,
			Metadata: &event.TaskExecutionMetadata{
				ExternalResources: externalResources,
			},
			TaskType:     "k8s-array",
			EventVersion: 1,
		}*/

		// TODO @hamersaw - determine node phase from ArrayNodePhase (ie. Queued, Running, Succeeded, Failed)
		taskExecutionEvent, err := buildTaskExecutionEvent(ctx, nCtx, idlcore.TaskExecution_RUNNING, taskPhaseVersion, externalResources)
		if err != nil {
			return handler.UnknownTransition, err
		}

		// TODO @hamersaw - pass eventConfig correctly
		if err := nCtx.EventsRecorder().RecordTaskEvent(ctx, taskExecutionEvent, &config.EventConfig{}); err != nil {
			logger.Errorf(ctx, "ArrayNode event recording failed: [%s]", err.Error())
			return handler.UnknownTransition, err
		}

		// TODO @hamersaw - only need to increment if arrayNodeState.Phase does not change
		//  if it does we can reset to 0
		arrayNodeState.TaskPhaseVersion = taskPhaseVersion+1
	}

	// update array node status
	if err := nCtx.NodeStateWriter().PutArrayNodeState(arrayNodeState); err != nil {
		logger.Errorf(ctx, "failed to store ArrayNode state with err [%s]", err.Error())
		return handler.UnknownTransition, err
	}

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(&handler.ExecutionInfo{})), nil
}

// Setup handles any initialization requirements for this handler
func (a *arrayNodeHandler) Setup(_ context.Context, _ handler.SetupContext) error {
	return nil
}

// New initializes a new arrayNodeHandler
func New(nodeExecutor interfaces.Node, scope promutils.Scope) (handler.Node, error) {
	// create k8s PluginState byte mocks to reuse instead of creating for each subNode evaluation
	pluginStateBytesNotStarted, err := bytesFromK8sPluginState(k8s.PluginState{Phase: k8s.PluginPhaseNotStarted})
	if err != nil {
		return nil, err
	}

	pluginStateBytesStarted, err := bytesFromK8sPluginState(k8s.PluginState{Phase: k8s.PluginPhaseStarted})
	if err != nil {
		return nil, err
	}

	arrayScope := scope.NewSubScope("array")
	return &arrayNodeHandler{
		metrics:                    newMetrics(arrayScope),
		nodeExecutor:               nodeExecutor,
		pluginStateBytesNotStarted: pluginStateBytesNotStarted,
		pluginStateBytesStarted:    pluginStateBytesStarted,
	}, nil
}

func (a *arrayNodeHandler) buildArrayNodeContext(ctx context.Context, nCtx interfaces.NodeExecutionContext, arrayNodeState *interfaces.ArrayNodeState, arrayNode v1alpha1.ExecutableArrayNode, subNodeIndex int, currentParallelism *uint32) (
	interfaces.Node, executors.ExecutionContext, executors.DAGStructure, executors.NodeLookup, *v1alpha1.NodeSpec, *v1alpha1.NodeStatus, *arrayEventRecorder, error) {

	nodePhase := v1alpha1.NodePhase(arrayNodeState.SubNodePhases.GetItem(subNodeIndex))
	taskPhase := int(arrayNodeState.SubNodeTaskPhases.GetItem(subNodeIndex))

	// need to initialize the inputReader everytime to ensure TaskHandler can access for cache lookups / population
	inputLiteralMap, err := constructLiteralMap(ctx, nCtx.InputReader(), subNodeIndex)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}

	inputReader := newStaticInputReader(nCtx.InputReader(), inputLiteralMap)

	// if node has not yet started we automatically set to NodePhaseQueued to skip input resolution
	if nodePhase == v1alpha1.NodePhaseNotYetStarted {
		// TODO @hamersaw how does this work with fastcache?
		// to supprt fastcache we'll need to override the bindings to BindingScalars for the input resolution on the nCtx
		// that way we resolution is just reading a literal ... but does this still write a file then?!?
		nodePhase = v1alpha1.NodePhaseQueued
	}

	// wrap node lookup
	subNodeSpec := *arrayNode.GetSubNodeSpec()

	subNodeID := fmt.Sprintf("%s-n%d", nCtx.NodeID(), subNodeIndex)
	subNodeSpec.ID = subNodeID
	subNodeSpec.Name = subNodeID

	// TODO - if we want to support more plugin types we need to figure out the best way to store plugin state
	//  currently just mocking based on node phase -> which works for all k8s plugins
	// we can not pre-allocated a bit array because max size is 256B and with 5k fanout node state = 1.28MB
	pluginStateBytes := a.pluginStateBytesStarted
	//if nodePhase == v1alpha1.NodePhaseQueued || nodePhase == v1alpha1.NodePhaseRetryableFailure {
	if taskPhase == int(core.PhaseUndefined) || taskPhase == int(core.PhaseRetryableFailure) {
		pluginStateBytes = a.pluginStateBytesNotStarted
	}

	// we set subDataDir and subOutputDir to the node dirs because flytekit automatically appends subtask
	// index. however when we check completion status we need to manually append index - so in all cases
	// where the node phase is not Queued (ie. task handler will launch task and init flytekit params) we
	// append the subtask index.
	/*var subDataDir, subOutputDir storage.DataReference
	if nodePhase == v1alpha1.NodePhaseQueued {
		subDataDir, subOutputDir, err = constructOutputReferences(ctx, nCtx)
	} else {
		subDataDir, subOutputDir, err = constructOutputReferences(ctx, nCtx, strconv.Itoa(i))
	}*/
	// TODO @hamersaw - this is a problem because cache lookups happen in NodePhaseQueued
	// so the cache hit items will be written to the wrong location
	//    can we just change flytekit appending the index onto the location?!?1
	currentAttempt := uint32(arrayNodeState.SubNodeRetryAttempts.GetItem(subNodeIndex))
	subDataDir, subOutputDir, err := constructOutputReferences(ctx, nCtx, strconv.Itoa(subNodeIndex), strconv.Itoa(int(currentAttempt)))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}

	subNodeStatus := &v1alpha1.NodeStatus{
		Phase:     nodePhase,
		DataDir:   subDataDir,
		OutputDir: subOutputDir,
		Attempts:  currentAttempt,
		SystemFailures: uint32(arrayNodeState.SubNodeSystemFailures.GetItem(subNodeIndex)),
		TaskNodeStatus: &v1alpha1.TaskNodeStatus{
			Phase: taskPhase,
			PluginState: pluginStateBytes,
		},
	}

	// initialize mocks
	arrayNodeLookup := newArrayNodeLookup(nCtx.ContextualNodeLookup(), subNodeID, &subNodeSpec, subNodeStatus)

	arrayExecutionContext := newArrayExecutionContext(nCtx.ExecutionContext(), subNodeIndex, currentParallelism, arrayNode.GetParallelism())

	arrayEventRecorder := newArrayEventRecorder()
	arrayNodeExecutionContextBuilder := newArrayNodeExecutionContextBuilder(a.nodeExecutor.GetNodeExecutionContextBuilder(),
		subNodeID, subNodeIndex, subNodeStatus, inputReader, arrayEventRecorder, currentParallelism, arrayNode.GetParallelism())
	arrayNodeExecutor := a.nodeExecutor.WithNodeExecutionContextBuilder(arrayNodeExecutionContextBuilder)

	return arrayNodeExecutor, arrayExecutionContext, &arrayNodeLookup, &arrayNodeLookup, &subNodeSpec, subNodeStatus, arrayEventRecorder, nil
}

func appendLiteral(name string, literal *idlcore.Literal, outputLiterals map[string]*idlcore.Literal, length int) {
	outputLiteral, exists := outputLiterals[name]
	if !exists {
		outputLiteral = &idlcore.Literal{
			Value: &idlcore.Literal_Collection{
				Collection: &idlcore.LiteralCollection{
					Literals: make([]*idlcore.Literal, 0, length),
				},
			},
		}

		outputLiterals[name] = outputLiteral
	}

	collection := outputLiteral.GetCollection()
	collection.Literals = append(collection.Literals, literal)
}

func buildTaskExecutionEvent(ctx context.Context, nCtx interfaces.NodeExecutionContext, taskPhase idlcore.TaskExecution_Phase, taskPhaseVersion uint32, externalResources []*event.ExternalResourceInfo) (*event.TaskExecutionEvent, error) {
	occurredAt, err := ptypes.TimestampProto(time.Now())
	if err != nil {
		return nil, err
	}

	nodeExecutionId := nCtx.NodeExecutionMetadata().GetNodeExecutionID()
	workflowExecutionId := nodeExecutionId.ExecutionId
	return &event.TaskExecutionEvent{
		TaskId: &idlcore.Identifier{
			ResourceType: idlcore.ResourceType_TASK,
			Project:      workflowExecutionId.Project,
			Domain:       workflowExecutionId.Domain,
			Name:         nCtx.NodeID(),
			Version:      "v1", // TODO @hamersaw - please
		},
		ParentNodeExecutionId: nCtx.NodeExecutionMetadata().GetNodeExecutionID(),
		RetryAttempt:          0, // ArrayNode will never retry 
		Phase:                 taskPhase,
		PhaseVersion:          taskPhaseVersion,
		OccurredAt:            occurredAt,
		Metadata: &event.TaskExecutionMetadata{
			ExternalResources: externalResources,
		},
		TaskType:     "k8s-array",
		EventVersion: 1,
	}, nil
}

func bytesFromK8sPluginState(pluginState k8s.PluginState) ([]byte, error) {
	buffer := make([]byte, 0, task.MaxPluginStateSizeBytes)
	bufferWriter := bytes.NewBuffer(buffer)

	codec := codex.GobStateCodec{}
	if err := codec.Encode(pluginState, bufferWriter); err != nil {
		return nil, err
	}

	return bufferWriter.Bytes(), nil
}

func constructOutputReferences(ctx context.Context, nCtx interfaces.NodeExecutionContext, postfix...string) (storage.DataReference, storage.DataReference, error) {
	subDataDir, err := nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetDataDir(), postfix...)
	if err != nil {
		return "", "", err
	}

	subOutputDir, err := nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetOutputDir(), postfix...)
	if err != nil {
		return "", "", err
	}

	return subDataDir, subOutputDir, nil
}
