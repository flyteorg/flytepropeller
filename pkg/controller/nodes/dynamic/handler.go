package dynamic

import (
	"context"
	"fmt"
	"time"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils/labeled"

	"github.com/lyft/flytepropeller/pkg/compiler"
	common2 "github.com/lyft/flytepropeller/pkg/compiler/common"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/catalog"

	"github.com/lyft/flytestdlib/promutils"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/errors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
)

type metrics struct {
	buildDynamicWorkflow   labeled.StopWatch
	retrieveDynamicJobSpec labeled.StopWatch
}

func newMetrics(scope promutils.Scope) metrics {
	return metrics{
		buildDynamicWorkflow:   labeled.NewStopWatch("build_dynamic_workflow", "Overhead for building a dynamic workflow in memory.", time.Microsecond, scope),
		retrieveDynamicJobSpec: labeled.NewStopWatch("retrieve_dynamic_spec", "Overhead of downloading and unmarshaling dynamic job spec", time.Microsecond, scope),
	}
}

type dynamicNodeHandler struct {
	*task.Handler
	metrics      metrics
	nodeExecutor executors.Node
}

func (d dynamicNodeHandler) Handle(ctx context.Context, nCtx handler.NodeExecutionContext) (handler.Transition, error) {

	ds := nCtx.NodeStateReader().GetDynamicNodeState()
	switch ds.Phase {
	case v1alpha1.DynamicNodePhaseExecuting:
		dynamicWF, _, err := d.buildContextualDynamicWorkflow(ctx, nCtx)
		if err != nil {
			return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure("DynamicWorkflowBuildFailed", err.Error(), nil)), nil
		}

		trns, err := d.progressDynamicWorkflow(ctx, dynamicWF, nCtx)
		if err != nil {
			return handler.UnknownTransition, err
		}

		// TODO: This is a hack to set parent task execution id, we should move to node-node relationship.
		execID, err := d.getTaskExecutionIdentifier(ctx, nCtx)
		if err != nil {
			return handler.UnknownTransition, err
		}
		// TODO we need to set parentTaskID in this case
		if trns.Info().Info == nil {
			pInfo := trns.Info()
			pInfo.Info = &handler.ExecutionInfo{}
			trns = handler.DoTransition(trns.Type(), pInfo)
		}
		trns.Info().Info.DynamicNodeInfo = &handler.DynamicNodeInfo{
			ParentTaskID: execID,
		}
		if trns.Info().Phase == handler.EPhaseSuccess {
			logger.Infof(ctx, "dynamic workflow node has succeeded, will call on success handler for parent node [%s]", nCtx.NodeID())
			outputPaths, err := task.NewRemoteFileOutputPaths(ctx, nCtx.NodeStatus().GetDataDir(), nCtx.DataStore())
			if err != nil {
				return handler.UnknownTransition, err
			}
			outputReader := ioutils.NewRemoteFileOutputReader(ctx, nCtx.DataStore(), outputPaths, nCtx.MaxDatasetSizeBytes())
			ee, err := d.Handler.ValidateOutputAndCacheAdd(ctx, nCtx.InputReader(), outputReader, nCtx.TaskReader(), catalog.Metadata{
				TaskExecutionIdentifier: execID,
			})
			if err != nil {
				return handler.UnknownTransition, err
			}
			if ee != nil {
				if ee.IsRecoverable {
					return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRetryableFailureErr(ee.ExecutionError, nil)), nil
				}
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailureErr(ee.ExecutionError, nil)), nil
			}
		}
		return trns, nil
	default:
		// It seems parent node is still running, lets call handle for parent node
		trns, err := d.Handler.Handle(ctx, nCtx)
		if err != nil {
			return trns, err
		}

		if trns.Info().Phase != handler.EPhaseSuccess {
			return trns, err
		}

		f, err := task.NewRemoteFutureFileReader(ctx, nCtx.NodeStatus().GetDataDir(), nCtx.DataStore())
		if err != nil {
			return handler.UnknownTransition, err
		}
		// If the node succeeded, check if it generated a futures.pb file to execute.
		if ok, err := f.Exists(ctx); err != nil {
			return handler.UnknownTransition, err
		} else if ok {
			// Mark the node that parent node has completed and a dynamic node executing its child nodes. Next time check node status is called, it'll go
			// directly to progress the dynamically generated workflow.
			if err := nCtx.NodeStateWriter().PutDynamicNodeState(handler.DynamicNodeState{
				Phase: v1alpha1.DynamicNodePhaseExecuting,
			}); err != nil {
				return handler.UnknownTransition, err
			}
			// There is a futures file, so we need to continue running the node with the modified state
			return handler.DoTransition(trns.Type(), handler.PhaseInfoRunning(trns.Info().Info)), nil
		}
		return trns, nil
	}
}

func (d dynamicNodeHandler) Abort(ctx context.Context, nCtx handler.NodeExecutionContext) error {

	ds := nCtx.NodeStateReader().GetDynamicNodeState()
	switch ds.Phase {
	case v1alpha1.DynamicNodePhaseExecuting:
		dynamicWF, isDynamic, err := d.buildContextualDynamicWorkflow(ctx, nCtx)
		if err != nil {
			return err
		}

		if !isDynamic {
			return nil
		}

		return d.nodeExecutor.AbortHandler(ctx, dynamicWF, dynamicWF.StartNode())
	default:
		// The parent node has not yet completed, so we will abort the parent node
		return d.Handler.Abort(ctx, nCtx)
	}
}

// This is a weird method. We should always finalize before we set the dynamic parent node phase as complete?
// TODO we are finalizing the parent node only after sub tasks are completed
func (d dynamicNodeHandler) Finalize(ctx context.Context, nCtx handler.NodeExecutionContext) error {
	return d.Handler.Finalize(ctx, nCtx)
}

func (d dynamicNodeHandler) buildDynamicWorkflowTemplate(ctx context.Context, djSpec *core.DynamicJobSpec, nCtx handler.NodeExecutionContext) (
	*core.WorkflowTemplate, error) {

	iface, err := underlyingInterface(nCtx.Workflow(), nCtx.Node())
	if err != nil {
		return nil, err
	}

	// Modify node IDs to include lineage, the entire system assumes node IDs are unique per parent WF.
	// We keep track of the original node ids because that's where inputs are written to.
	parentNodeID := nCtx.NodeID()
	for _, n := range djSpec.Nodes {
		newID, err := hierarchicalNodeID(parentNodeID, n.Id)
		if err != nil {
			return nil, err
		}

		// Instantiate a nodeStatus using the modified name but set its data directory using the original name.
		subNodeStatus := nCtx.NodeStatus().GetNodeExecutionStatus(newID)
		originalNodePath, err := nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetDataDir(), n.Id)
		if err != nil {
			return nil, err
		}

		subNodeStatus.SetDataDir(originalNodePath)
		subNodeStatus.ResetDirty()

		n.Id = newID
	}

	if nCtx.TaskReader().GetTaskID() != nil {
		// If the parent is a task, pass down data children nodes should inherit.
		parentTask, err := nCtx.TaskReader().Read(ctx)
		if err != nil {
			return nil, errors.Wrapf(errors.CausedByError, nCtx.NodeID(), err, "Failed to find task [%v].", nCtx.TaskReader().GetTaskID())
		}

		for _, t := range djSpec.Tasks {
			if t.GetContainer() != nil && parentTask.GetContainer() != nil {
				t.GetContainer().Config = append(t.GetContainer().Config, parentTask.GetContainer().Config...)
			}
		}
	}

	for _, o := range djSpec.Outputs {
		err = updateBindingNodeIDsWithLineage(parentNodeID, o.Binding)
		if err != nil {
			return nil, err
		}
	}

	return &core.WorkflowTemplate{
		Id: &core.Identifier{
			Project:      nCtx.NodeExecutionMetadata().GetExecutionID().Project,
			Domain:       nCtx.NodeExecutionMetadata().GetExecutionID().Domain,
			Version:      rand.String(10),
			Name:         rand.String(10),
			ResourceType: core.ResourceType_WORKFLOW,
		},
		Nodes:     djSpec.Nodes,
		Outputs:   djSpec.Outputs,
		Interface: iface,
	}, nil
}

func (d dynamicNodeHandler) buildContextualDynamicWorkflow(ctx context.Context, nCtx handler.NodeExecutionContext) (dynamicWf v1alpha1.ExecutableWorkflow, isDynamic bool, err error) {

	t := d.metrics.buildDynamicWorkflow.Start(ctx)
	defer t.Stop()

	f, err := task.NewRemoteFutureFileReader(ctx, nCtx.NodeStatus().GetDataDir(), nCtx.DataStore())
	if err != nil {
		return nil, false, err
	}
	// We know for sure that futures file was generated. Lets read it
	djSpec, err := f.Read(ctx)
	if err != nil {
		return nil, false, errors.Wrapf(errors.RuntimeExecutionError, nCtx.NodeID(), err, "unable to read futures file, maybe corrupted")
	}

	var closure *core.CompiledWorkflowClosure
	wf, err := d.buildDynamicWorkflowTemplate(ctx, djSpec, nCtx)
	if err != nil {
		return nil, true, err
	}

	compiledTasks, err := compileTasks(ctx, djSpec.Tasks)
	if err != nil {
		return nil, true, err
	}

	// TODO: This will currently fail if the WF references any launch plans
	closure, err = compiler.CompileWorkflow(wf, djSpec.Subworkflows, compiledTasks, []common2.InterfaceProvider{})
	if err != nil {
		return nil, true, err
	}

	subwf, err := k8s.BuildFlyteWorkflow(closure, nil, nil, "")
	if err != nil {
		return nil, true, err
	}

	return newContextualWorkflow(nCtx.Workflow(), subwf, nCtx.NodeStatus(), subwf.Tasks, subwf.SubWorkflows), true, nil
}

func (d dynamicNodeHandler) progressDynamicWorkflow(ctx context.Context, dynamicWorkflow v1alpha1.ExecutableWorkflow, nCtx handler.NodeExecutionContext) (handler.Transition, error) {

	state, err := d.nodeExecutor.RecursiveNodeHandler(ctx, dynamicWorkflow, dynamicWorkflow.StartNode())
	if err != nil {
		return handler.UnknownTransition, err
	}

	if state.HasFailed() {
		if dynamicWorkflow.GetOnFailureNode() != nil {
			// TODO Once we migrate to closure node we need to handle subworkflow using the subworkflow handler
			logger.Errorf(ctx, "We do not support failure nodes in dynamic workflow today")
		}
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure("DynamicWorkflowFailure", state.Err.Error(), nil)), err
	}

	if state.IsComplete() {
		// If the WF interface has outputs, validate that the outputs file was written.
		if outputBindings := dynamicWorkflow.GetOutputBindings(); len(outputBindings) > 0 {
			endNodeStatus := dynamicWorkflow.GetNodeExecutionStatus(v1alpha1.EndNodeID)
			if endNodeStatus == nil {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure("MalformedDynamicWorkflow", "no end-node found in dynamic workflow", nil)), nil
			}

			sourcePath := v1alpha1.GetOutputsFile(endNodeStatus.GetDataDir())
			if metadata, err := nCtx.DataStore().Head(ctx, sourcePath); err == nil {
				if !metadata.Exists() {
					return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure("DynamicWorkflowOutputsNotFound", " is expected to produce outputs but no outputs file was written to %v.", nil)), nil
				}
			} else {
				return handler.UnknownTransition, err
			}

			destinationPath := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetDataDir())
			if err := nCtx.DataStore().CopyRaw(ctx, sourcePath, destinationPath, storage.Options{}); err != nil {
				return handler.DoTransition(handler.TransitionTypeEphemeral,
					handler.PhaseInfoFailure(errors.OutputsNotFoundError.String(),
						fmt.Sprintf("Failed to copy subworkflow outputs from [%v] to [%v]", sourcePath, destinationPath), nil),
				), nil
			}
		}

		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(nil)), nil
	}

	if state.PartiallyComplete() {
		if err := nCtx.EnqueueOwner(); err != nil {
			return handler.UnknownTransition, err
		}
	}

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRunning(nil)), nil
}

func (d dynamicNodeHandler) getTaskExecutionIdentifier(_ context.Context, nCtx handler.NodeExecutionContext) (*core.TaskExecutionIdentifier, error) {

	return &core.TaskExecutionIdentifier{
		TaskId:       nCtx.TaskReader().GetTaskID(),
		RetryAttempt: nCtx.CurrentAttempt(),
		NodeExecutionId: &core.NodeExecutionIdentifier{
			NodeId:      nCtx.NodeID(),
			ExecutionId: nCtx.NodeExecutionMetadata().GetExecutionID().WorkflowExecutionIdentifier,
		},
	}, nil
}

func New(underlying *task.Handler, nodeExecutor executors.Node, scope promutils.Scope) handler.Node {

	return &dynamicNodeHandler{
		Handler:      underlying,
		metrics:      newMetrics(scope),
		nodeExecutor: nodeExecutor,
	}
}
