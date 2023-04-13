package array

import (
	"bytes"
	"context"
	"fmt"
	"strconv"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	//"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/validators"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/interfaces"
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
	metrics      metrics
	nodeExecutor interfaces.Node
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
	return nil // TODO @hamersaw - implement finalize
}

// FinalizeRequired defines whether or not this handler requires finalize to be called on
// node completion
func (a *arrayNodeHandler) FinalizeRequired() bool {
	return true // TODO @hamersaw - implement finalize required
}

// Handle is responsible for transitioning and reporting node state to complete the node defined
// by the NodeExecutionContext
func (a *arrayNodeHandler) Handle(ctx context.Context, nCtx interfaces.NodeExecutionContext) (handler.Transition, error) {
	arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

	// TODO @hamersaw - handle array node
	// the big question right now is if we make a DAG with everything or call a separate DAG for each individual task
	//   need to do much more thinking on this - cleaner = a single DAG / maybe easier = DAG for each
	// single:
	//   + can still add envVars - override in ArrayNodeExectionContext
	// each:
	//   + add envVars on ExecutionContext
	//   - need to manage

	var inputs *idlcore.LiteralMap

	switch arrayNodeState.Phase {
	case v1alpha1.ArrayNodePhaseNone:
		// identify and validate array node input value lengths
		literalMap, err := nCtx.InputReader().Get(ctx)
		if err != nil {
			return handler.UnknownTransition, err // TODO @hamersaw fail
		}

		size := -1
		for _, variable := range literalMap.Literals {
			literalType := validators.LiteralTypeForLiteral(variable)
			switch literalType.Type.(type) {
			case *idlcore.LiteralType_CollectionType:
				collection := variable.GetCollection()
				collectionLength := len(collection.Literals)

				if size == -1 {
					size = collectionLength
				} else if size != collectionLength {
					// TODO @hamersaw - return error
				}
			}
		}

		if size == -1 {
			// TODO @hamersaw return
		}

		// initialize ArrayNode state
		arrayNodeState.Phase = v1alpha1.ArrayNodePhaseExecuting
		arrayNodeState.SubNodePhases, err = bitarray.NewCompactArray(uint(size), bitarray.Item(len(core.Phases)-1))
		if err != nil {
			// TODO @hamersaw fail
		}
		// TODO @hamersaw - init SystemFailures and RetryAttempts as well
		//   do we want to abstract this? ie. arrayNodeState.GetStats(subNodeIndex) (phase, systemFailures, ...)

		//fmt.Printf("HAMERSAW - created SubNodePhases with length '%d:%d'\n", size, len(arrayNodeState.SubNodePhases.GetItems()))
	case v1alpha1.ArrayNodePhaseExecuting:
		// process array node subnodes
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)
			fmt.Printf("HAMERSAW - TODO evaluating node '%d' in phase '%d'\n", i, nodePhase)

			// TODO @hamersaw - fix
			if nodePhase == v1alpha1.NodePhaseSucceeded || nodePhase == v1alpha1.NodePhaseFailed || nodePhase == v1alpha1.NodePhaseTimedOut || nodePhase == v1alpha1.NodePhaseSkipped {
				continue
			}

			/*if nodes.IsTerminalNodePhase(nodePhase) {
				continue
			}*/

			// TODO @hamersaw - do we need to init input readers every time?
			literalMap, err := constructLiteralMap(ctx, nCtx.InputReader(), i, inputs)
			if err != nil {
				logger.Errorf(ctx, "HAMERSAW - %+v", err)
				// TODO @hamersaw - return err
			}

			inputReader := newStaticInputReader(nCtx.InputReader(), &literalMap)

			if nodePhase == v1alpha1.NodePhaseNotYetStarted {
				// set nodePhase to Queued to skip resolving inputs but still allow cache lookups
				nodePhase = v1alpha1.NodePhaseQueued
			}

			// wrap node lookup
			subNodeSpec := *arrayNode.GetSubNodeSpec()

			subNodeID := fmt.Sprintf("%s-n%d", nCtx.NodeID(), i)
			subNodeSpec.ID = subNodeID
			subNodeSpec.Name = subNodeID

			// TODO @hamersaw - is this right?!?! it's certainly HACKY AF - maybe we persist pluginState.Phase and PluginPhase
			pluginState := k8s.PluginState{
			}
			if nodePhase == v1alpha1.NodePhaseQueued {
				pluginState.Phase = k8s.PluginPhaseNotStarted
			} else {
				pluginState.Phase = k8s.PluginPhaseStarted
			}

			buffer := make([]byte, 0, 256)
			bufferWriter := bytes.NewBuffer(buffer)

			codec := codex.GobStateCodec{}
			if err := codec.Encode(pluginState, bufferWriter); err != nil {
				logger.Errorf(ctx, "HAMERSAW - %+v", err)
			}

			// we set subDataDir and subOutputDir to the node dirs because flytekit automatically appends subtask
			// index. however when we check completion status we need to manually append index - so in all cases
			// where the node phase is not Queued (ie. task handler will launch task and init flytekit params) we
			// append the subtask index.
			var subDataDir, subOutputDir storage.DataReference
			if nodePhase == v1alpha1.NodePhaseQueued {
				subDataDir = nCtx.NodeStatus().GetDataDir()
				subOutputDir = nCtx.NodeStatus().GetOutputDir()
			} else {
				subDataDir, err = nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetDataDir(), strconv.Itoa(i)) // TODO @hamersaw - constructOutputReference?
				if err != nil {
					logger.Errorf(ctx, "HAMERSAW - %+v", err)
					// TODO @hamersaw - return err
				}

				subOutputDir, err = nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetOutputDir(), strconv.Itoa(i)) // TODO @hamersaw - constructOutputReference?
				if err != nil {
					logger.Errorf(ctx, "HAMERSAW - %+v", err)
					// TODO @hamersaw - return err
				}
			}

			subNodeStatus := &v1alpha1.NodeStatus{
				Phase: nodePhase,
				TaskNodeStatus: &v1alpha1.TaskNodeStatus{
					// TODO @hamersaw - to get caching working we need to set to Queued to force cache lookup
					// once fastcache is done we dont care about the TaskNodeStatus
					Phase: int(core.Phases[core.PhaseRunning]),
					PluginState: bufferWriter.Bytes(),
				},
				DataDir:   subDataDir,
				OutputDir: subOutputDir,
				// TODO @hamersaw - fill out systemFailures, retryAttempt etc
			}

			// TODO @hamersaw - can probably create a single arrayNodeLookup with all the subNodeIDs
			arrayNodeLookup := newArrayNodeLookup(nCtx.ContextualNodeLookup(), subNodeID, &subNodeSpec, subNodeStatus)

			// execute subNode through RecursiveNodeHandler
			_, err = a.nodeExecutor.RecursiveNodeHandlerWithNodeContextModifier(ctx, nCtx.ExecutionContext(), &arrayNodeLookup, &arrayNodeLookup, &subNodeSpec,
			func (nCtx interfaces.NodeExecutionContext) interfaces.NodeExecutionContext {
				if nCtx.NodeID() == subNodeID {
					return newArrayNodeExecutionContext(nCtx, inputReader, i)
				}

				return nCtx
			})

			if err != nil {
				logger.Errorf(ctx, "HAMERSAW - %+v", err)
				// TODO @hamersaw fail
			}

			fmt.Printf("HAMERSAW - node phase transition %d -> %d\n", nodePhase, subNodeStatus.GetPhase())
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
			subDataDir, err := nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetDataDir(), strconv.Itoa(i)) // TODO @hamersaw - constructOutputReference?
			if err != nil {
				logger.Errorf(ctx, "HAMERSAW - %+v", err)
				// TODO @hamersaw - return err
			}

			subOutputDir, err := nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetOutputDir(), strconv.Itoa(i)) // TODO @hamersaw - constructOutputReference?
			if err != nil {
				logger.Errorf(ctx, "HAMERSAW - %+v", err)
				// TODO @hamersaw - return err
			}

			// checkpoint paths are not computed here because this function is only called when writing
			// existing cached outputs. if this functionality changes this will need to be revisited.
			outputPaths := ioutils.NewCheckpointRemoteFilePaths(ctx, nCtx.DataStore(), subOutputDir, ioutils.NewRawOutputPaths(ctx, subDataDir), "")
			reader := ioutils.NewRemoteFileOutputReader(ctx, nCtx.DataStore(), outputPaths, int64(999999999))

			// read outputs
			outputs, executionError, err := reader.Read(ctx)
			if err != nil {
				logger.Warnf(ctx, "Failed to read output for subtask [%v]. Error: %v", i, err)
				//return workqueue.WorkStatusFailed, err // TODO @hamersaw -return error
			}

			if executionError == nil && outputs != nil {
				for name, literal := range outputs.GetLiterals() {
					existingVal, found := outputLiterals[name]
					var list *idlcore.LiteralCollection
					if found {
						list = existingVal.GetCollection()
					} else {
						list = &idlcore.LiteralCollection{
							Literals: make([]*idlcore.Literal, 0, len(arrayNodeState.SubNodePhases.GetItems())),
						}

						existingVal = &idlcore.Literal{
							Value: &idlcore.Literal_Collection{
								Collection: list,
							},
						}
					}

					list.Literals = append(list.Literals, literal)
					outputLiterals[name] = existingVal
				}
			}
		}

		// TODO @hamersaw - collect outputs and write as List[]
		fmt.Printf("HAMERSAW - final outputs %+v\n", idlcore.LiteralMap{Literals: outputLiterals})
		outputLiteralMap := &idlcore.LiteralMap{
			Literals: outputLiterals,
		}

		outputFile := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
		if err := nCtx.DataStore().WriteProtobuf(ctx, outputFile, storage.Options{}, outputLiteralMap); err != nil {
			// TODO @hamersaw return error
			//return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM, "WriteOutputsFailed",
			//	fmt.Sprintf("failed to write signal value to [%v] with error [%s]", outputFile, err.Error()), nil)), nil
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
	return nil // TODO @hamersaw - implement setup
}

// New initializes a new arrayNodeHandler
func New(nodeExecutor interfaces.Node, scope promutils.Scope) handler.Node {
	arrayScope := scope.NewSubScope("array")
	return &arrayNodeHandler{
		metrics:      newMetrics(arrayScope),
		nodeExecutor: nodeExecutor,
	}
}
