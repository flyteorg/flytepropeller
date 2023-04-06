package array

import (
	"context"
	"fmt"

	idlcore "github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"

	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/core"
	"github.com/flyteorg/flyteplugins/go/tasks/pluginmachinery/io"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler/validators"
	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"

	"github.com/flyteorg/flytestdlib/bitarray"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/promutils"
)

//go:generate mockery -all -case=underscore

// arrayNodeHandler is a handle implementation for processing array nodes
type arrayNodeHandler struct {
	metrics      metrics
	nodeExecutor executors.Node
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
func (a *arrayNodeHandler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext, reason string) error {
	return nil // TODO @hamersaw - implement abort
}

// Finalize completes the array node defined in the NodeExecutionContext
func (a *arrayNodeHandler) Finalize(ctx context.Context, _ handler.NodeExecutionContext) error {
	return nil // TODO @hamersaw - implement finalize
}

// FinalizeRequired defines whether or not this handler requires finalize to be called on
// node completion
func (a *arrayNodeHandler) FinalizeRequired() bool {
	return true // TODO @hamersaw - implement finalize required
}

// Handle is responsible for transitioning and reporting node state to complete the node defined
// by the NodeExecutionContext
func (a *arrayNodeHandler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {
	//arrayNode := nCtx.Node().GetArrayNode()
	arrayNodeState := nCtx.NodeStateReader().GetArrayNodeState()

	// TODO @hamersaw - handle array node
	// the big question right now is if we make a DAG with everything or call a separate DAG for each individual task
	//   need to do much more thinking on this - cleaner = a single DAG / maybe easier = DAG for each
	// single:
	//   + can still add envVars - override in ArrayNodeExectionContext
	// each:
	//   + add envVars on ExecutionContext
	//   - need to manage

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

		fmt.Printf("HAMERSAW - created SubNodePhases with length '%d:%d'\n", size, len(arrayNodeState.SubNodePhases.GetItems()))
	case v1alpha1.ArrayNodePhaseExecuting:
		// process array node subnodes
		for i, nodePhaseUint64 := range arrayNodeState.SubNodePhases.GetItems() {
			nodePhase := v1alpha1.NodePhase(nodePhaseUint64)
			fmt.Printf("HAMERSAW - TODO evaluating node '%d' in phase '%d'\n", i, nodePhase)

			// TODO @hamersaw - fix
			/*if nodes.IsTerminalNodePhase(nodePhase) {
				continue
			}*/

			var inputReader io.InputReader
			if nodePhase == v1alpha1.NodePhaseNotYetStarted { // TODO @hamersaw - need to do this for PhaseSucceeded as well?!?! to write cache outputs once fastcache is in
				// create input readers and set nodePhase to Queued to skip resolving inputs but still allow cache lookups
				// TODO @hamersaw - create input readers
				nodePhase = v1alpha1.NodePhaseQueued
			}

			// wrap node lookup
			subNodeID := fmt.Sprintf("%s-n%d", nCtx.NodeID(), i)
			subNodeSpec := &v1alpha1.NodeSpec{
				ID:   subNodeID,
				Name: subNodeID,
			} // TODO @hamersaw - compile this in ArrayNodeSpec?
			subNodeStatus := &v1alpha1.NodeStatus{
				Phase: nodePhase,
				/*TaskNodeStatus: &v1alpha1.TaskNodeStatus{
					Phase: nodePhase, // used for cache lookups - once fastcache is done we dont care about the TaskNodeStatus
				},*/
				// TODO @hamersaw - fill out systemFailures, retryAttempt etc
			}

			// TODO @hamersaw - can probably create a single arrayNodeLookup with all the subNodeIDs
			arrayNodeLookup := newArrayNodeLookup(nCtx.ContextualNodeLookup(), subNodeID, subNodeSpec, subNodeStatus)

			// create base NodeExecutionContext
			nodeExecutionContext, err := a.nodeExecutor.NewNodeExecutionContext(ctx, nCtx.ExecutionContext(), arrayNodeLookup, subNodeID)
			if err != nil {
				// TODO @hamersaw fail
			}

			// create new arrayNodeExecutionContext to override for array task execution
			arrayNodeExecutionContext := newArrayNodeExecutionContext(nodeExecutionContext, inputReader)

			// execute subNode through RecursiveNodeHandler
			// TODO @hamersaw -  either
			//  (1) add func to create nodeExecutionContext to RecursiveNodeHandler
			//  (2) create nodeExecutionContext before call to RecursiveNodeHandler
			//      can do with small wrapper function call
			nodeStatus, err := a.nodeExecutor.RecursiveNodeHandler(ctx, arrayNodeExecutionContext, &arrayNodeLookup, &arrayNodeLookup, subNodeSpec)
			if err != nil {
				// TODO @hamersaw fail
			}

			// handleNode / abort / finalize task nodeExecutionContext and Handler as parameters - THIS IS THE ENTRYPOINT WE'RE LOOKING FOR
		}

		// TODO @hamersaw - determine summary phases

		arrayNodeState.Phase = v1alpha1.ArrayNodePhaseSucceeding
	case v1alpha1.ArrayNodePhaseFailing:
		// TODO @hamersaw - abort everything!
	case v1alpha1.ArrayNodePhaseSucceeding:
		// TODO @hamersaw - collect outputs
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
func New(nodeExecutor executors.Node, scope promutils.Scope) handler.Node {
	arrayScope := scope.NewSubScope("array")
	return &arrayNodeHandler{
		metrics:      newMetrics(arrayScope),
		nodeExecutor: nodeExecutor,
	}
}
