package array

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/validators"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/errors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/k8s"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/codex"

	"github.com/flyteorg/flytestdlib/bitarray"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
	"github.com/flyteorg/flytestdlib/storage"
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
	return nil // TODO @hamersaw - implement abort
}

// Finalize completes the array node defined in the NodeExecutionContext
func (a *arrayNodeHandler) Finalize(ctx context.Context, _ interfaces.NodeExecutionContext) error {
	return nil // TODO @hamersaw - implement finalize - clear node data?!?!
}

// FinalizeRequired defines whether or not this handler requires finalize to be called on
// node completion
func (a *arrayNodeHandler) FinalizeRequired() bool {
	return false
}

// Handle is responsible for transitioning and reporting node state to complete the node defined
// by the NodeExecutionContext
func (a *arrayNodeHandler) Handle(ctx context.Context, nCtx interfaces.NodeExecutionContext) (handler.Transition, error) {
	arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

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
		arrayNodeState.SubNodePhases, err = bitarray.NewCompactArray(uint(size), bitarray.Item(len(core.Phases)-1))
		if err != nil {
			return handler.UnknownTransition, err
		}

		// TODO @hamersaw - init SystemFailures and RetryAttempts as well
		//   do we want to abstract this? ie. arrayNodeState.GetStats(subNodeIndex) (phase, systemFailures, ...)

		//fmt.Printf("HAMERSAW - created SubNodePhases with length '%d:%d'\n", size, len(arrayNodeState.SubNodePhases.GetItems()))
		arrayNodeState.Phase = v1alpha1.ArrayNodePhaseExecuting
	case v1alpha1.ArrayNodePhaseExecuting:
		// process array node subnodes
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)
			//fmt.Printf("HAMERSAW - evaluating node '%d' in phase '%d'\n", i, nodePhase)

			// TODO @hamersaw fix - do not process nodes in terminal state
			//if nodes.IsTerminalNodePhase(nodePhase) {
			if nodePhase == v1alpha1.NodePhaseSucceeded || nodePhase == v1alpha1.NodePhaseFailed || nodePhase == v1alpha1.NodePhaseTimedOut || nodePhase == v1alpha1.NodePhaseSkipped {
				continue
			}

			// initialize input reader if NodePhaseNotyetStarted or NodePhaseSucceeding for cache lookup and population 
			var inputLiteralMap *idlcore.LiteralMap
			var err error
			if nodePhase == v1alpha1.NodePhaseNotYetStarted || nodePhase == v1alpha1.NodePhaseSucceeding {
				inputLiteralMap, err = constructLiteralMap(ctx, nCtx.InputReader(), i)
				if err != nil {
					return handler.UnknownTransition, err
				}
			}

			inputReader := newStaticInputReader(nCtx.InputReader(), inputLiteralMap)

			// if node has not yet started we automatically set to NodePhaseQueued to skip input resolution
			if nodePhase == v1alpha1.NodePhaseNotYetStarted {
				// TODO @hamersaw how does this work with fastcache?
				nodePhase = v1alpha1.NodePhaseQueued
			}

			// wrap node lookup
			subNodeSpec := *arrayNode.GetSubNodeSpec()

			subNodeID := fmt.Sprintf("%s-n%d", nCtx.NodeID(), i)
			subNodeSpec.ID = subNodeID
			subNodeSpec.Name = subNodeID

			// TODO @hamersaw - store task phase and use to mock plugin state
			// TODO - if we want to support more plugin types we need to figure out the best way to store plugin state
			//  currently just mocking based on node phase -> which works for all k8s plugins
			// we can not pre-allocated a bit array because max size is 256B and with 5k fanout node state = 1.28MB
			pluginStateBytes := a.pluginStateBytesStarted
			if nodePhase == v1alpha1.NodePhaseQueued {
				pluginStateBytes = a.pluginStateBytesNotStarted
			}

			// we set subDataDir and subOutputDir to the node dirs because flytekit automatically appends subtask
			// index. however when we check completion status we need to manually append index - so in all cases
			// where the node phase is not Queued (ie. task handler will launch task and init flytekit params) we
			// append the subtask index.
			var subDataDir, subOutputDir storage.DataReference
			if nodePhase == v1alpha1.NodePhaseQueued {
				subDataDir, subOutputDir, err = constructOutputReferences(ctx, nCtx)
			} else {
				subDataDir, subOutputDir, err = constructOutputReferences(ctx, nCtx, strconv.Itoa(i))
			}

			if err != nil {
				return handler.UnknownTransition, err
			}

			subNodeStatus := &v1alpha1.NodeStatus{
				Phase: nodePhase,
				TaskNodeStatus: &v1alpha1.TaskNodeStatus{
					// TODO @hamersaw - to get caching working we need to set to Queued to force cache lookup
					// once fastcache is done we dont care about the TaskNodeStatus
					Phase: int(core.Phases[core.PhaseRunning]),
					PluginState: pluginStateBytes,
				},
				DataDir:   subDataDir,
				OutputDir: subOutputDir,
				// TODO @hamersaw - fill out systemFailures, retryAttempt etc
			}

			arrayNodeLookup := newArrayNodeLookup(nCtx.ContextualNodeLookup(), subNodeID, &subNodeSpec, subNodeStatus)

			// execute subNode through RecursiveNodeHandler
			arrayNodeExecutionContextBuilder := newArrayNodeExecutionContextBuilder(a.nodeExecutor.GetNodeExecutionContextBuilder(), subNodeID, i, inputReader)
			arrayNodeExecutor := a.nodeExecutor.WithNodeExecutionContextBuilder(arrayNodeExecutionContextBuilder)
			_, err = arrayNodeExecutor.RecursiveNodeHandler(ctx, nCtx.ExecutionContext(), &arrayNodeLookup, &arrayNodeLookup, &subNodeSpec)
			if err != nil {
				return handler.UnknownTransition, err
			}

			//fmt.Printf("HAMERSAW - node phase transition %d -> %d\n", nodePhase, subNodeStatus.GetPhase())
			arrayNodeState.SubNodePhases.SetItem(i, uint64(subNodeStatus.GetPhase()))
		}

		// TODO @hamersaw - determine summary phases
		succeeded := true
		for _, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)
			if nodePhase != v1alpha1.NodePhaseSucceeded {
				succeeded = false
				break
			}
		}

		if succeeded {
			arrayNodeState.Phase = v1alpha1.ArrayNodePhaseSucceeding
		}
	case v1alpha1.ArrayNodePhaseFailing:
		// TODO @hamersaw - abort everything!
	case v1alpha1.ArrayNodePhaseSucceeding:
		outputLiterals := make(map[string]*idlcore.Literal)
		for i, _ := range arrayNodeState.SubNodePhases.GetItems() {
			// initialize subNode reader
			subDataDir, subOutputDir, err := constructOutputReferences(ctx, nCtx, strconv.Itoa(i))
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
				return handler.UnknownTransition, executionErr
			}

			// copy individual subNode output literals into a collection of output literals
			for name, literal := range outputs.GetLiterals() {
				outputLiteral, exists := outputLiterals[name]
				if !exists {
					outputLiteral = &idlcore.Literal{
						Value: &idlcore.Literal_Collection{
							Collection: &idlcore.LiteralCollection{
								Literals: make([]*idlcore.Literal, 0, len(arrayNodeState.SubNodePhases.GetItems())),
							},
						},
					}

					outputLiterals[name] = outputLiteral
				}

				collection := outputLiteral.GetCollection()
				collection.Literals = append(collection.Literals, literal)
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

		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
	default:
		// TODO @hamersaw - fail
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
