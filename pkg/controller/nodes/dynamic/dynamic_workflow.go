package dynamic

import (
	"context"
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/util/rand"

	node_common "github.com/flyteorg/flytepropeller/pkg/controller/nodes/common"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/flyteorg/flytepropeller/pkg/compiler"
	"github.com/flyteorg/flytepropeller/pkg/compiler/common"
	"github.com/flyteorg/flytepropeller/pkg/compiler/transformers/k8s"
	"github.com/flyteorg/flytepropeller/pkg/controller/executors"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/handler"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/subworkflow/launchplan"
	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task"
	"github.com/flyteorg/flytepropeller/pkg/utils"
	"github.com/flyteorg/flytestdlib/errors"
	"github.com/flyteorg/flytestdlib/logger"
	"github.com/flyteorg/flytestdlib/storage"
)

type dynamicWorkflowContext struct {
	execContext        executors.ExecutionContext
	subWorkflow        v1alpha1.ExecutableWorkflow
	subWorkflowClosure *core.CompiledWorkflowClosure
	nodeLookup         executors.NodeLookup
	isDynamic          bool
	dynamicJobSpecURI  string
}

const dynamicWfNameTemplate = "dynamic_%s"

func setEphemeralNodeExecutionStatusAttributes(ctx context.Context, djSpec *core.DynamicJobSpec,
	nCtx handler.NodeExecutionContext, parentNodeStatus v1alpha1.ExecutableNodeStatus) error {
	if nCtx.ExecutionContext().GetEventVersion() != v1alpha1.EventVersion0 {
		return nil
	}

	currentAttemptStr := strconv.Itoa(int(nCtx.CurrentAttempt()))
	// Modify node IDs to include lineage, the entire system assumes node IDs are unique per parent WF.
	// We keep track of the original node ids because that's where flytekit inputs are written to in the case of legacy
	// map tasks. The modern map tasks do not write input files any longer and this entire piece of code can be removed.
	parentNodeID := nCtx.NodeID()
	for _, node := range djSpec.Nodes {
		nodeID := node.Id
		var subNodeStatus v1alpha1.ExecutableNodeStatus
		newID, err := hierarchicalNodeID(parentNodeID, currentAttemptStr, nodeID)
		if err != nil {
			return err
		}
		// Instantiate a nodeStatus using the modified name but set its data directory using the original name.
		subNodeStatus = parentNodeStatus.GetNodeExecutionStatus(ctx, newID)
		node.Id = newID

		// NOTE: This is the second step of 2-step-dynamic-node execution. Input dir for this step is generated by
		// parent task as a sub-directory(n.Id) in the parent node's output dir.
		originalNodePath, err := nCtx.DataStore().ConstructReference(ctx, nCtx.NodeStatus().GetOutputDir(), nodeID)
		if err != nil {
			return err
		}

		outputDir, err := nCtx.DataStore().ConstructReference(ctx, originalNodePath, strconv.Itoa(int(subNodeStatus.GetAttempts())))
		if err != nil {
			return err
		}

		subNodeStatus.SetDataDir(originalNodePath)
		subNodeStatus.SetOutputDir(outputDir)
	}

	return nil
}

func (d dynamicNodeTaskNodeHandler) buildDynamicWorkflowTemplate(ctx context.Context, djSpec *core.DynamicJobSpec,
	nCtx handler.NodeExecutionContext, parentNodeStatus v1alpha1.ExecutableNodeStatus) (*core.WorkflowTemplate, error) {

	iface, err := underlyingInterface(ctx, nCtx.TaskReader())
	if err != nil {
		return nil, err
	}

	err = setEphemeralNodeExecutionStatusAttributes(ctx, djSpec, nCtx, parentNodeStatus)
	if err != nil {
		return nil, err
	}

	parentNodeID := nCtx.NodeID()
	currentAttemptStr := strconv.Itoa(int(nCtx.CurrentAttempt()))
	if nCtx.TaskReader().GetTaskID() != nil {
		// If the parent is a task, pass down data children nodes should inherit.
		parentTask, err := nCtx.TaskReader().Read(ctx)
		if err != nil {
			return nil, errors.Wrapf("TaskReadFailed", err, "Failed to find task [%v].", nCtx.TaskReader().GetTaskID())
		}

		for _, t := range djSpec.Tasks {
			if t.GetContainer() != nil && parentTask.GetContainer() != nil {
				t.GetContainer().Config = append(t.GetContainer().Config, parentTask.GetContainer().Config...)
			}
		}
	}

	if nCtx.ExecutionContext().GetEventVersion() == v1alpha1.EventVersion0 {
		for _, o := range djSpec.Outputs {
			err = updateBindingNodeIDsWithLineage(parentNodeID, currentAttemptStr, o.Binding)
			if err != nil {
				return nil, err
			}
		}
	}
	return &core.WorkflowTemplate{
		Id: &core.Identifier{
			Project:      nCtx.NodeExecutionMetadata().GetNodeExecutionID().GetExecutionId().Project,
			Domain:       nCtx.NodeExecutionMetadata().GetNodeExecutionID().GetExecutionId().Domain,
			Name:         fmt.Sprintf(dynamicWfNameTemplate, nCtx.NodeExecutionMetadata().GetNodeExecutionID().NodeId),
			Version:      rand.String(10),
			ResourceType: core.ResourceType_WORKFLOW,
		},
		Nodes:     djSpec.Nodes,
		Outputs:   djSpec.Outputs,
		Interface: iface,
	}, nil
}

func (d dynamicNodeTaskNodeHandler) buildContextualDynamicWorkflow(ctx context.Context, nCtx handler.NodeExecutionContext) (dynamicWorkflowContext, error) {

	t := d.metrics.buildDynamicWorkflow.Start(ctx)
	defer t.Stop()
	f, err := task.NewRemoteFutureFileReader(ctx, nCtx.NodeStatus().GetOutputDir(), nCtx.DataStore())
	if err != nil {
		return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "failed to open futures file for reading")
	}

	// TODO: This is a hack to set parent task execution id, we should move to node-node relationship.
	execID := task.GetTaskExecutionIdentifier(nCtx)
	dynamicNodeStatus := nCtx.NodeStatus().GetNodeExecutionStatus(ctx, dynamicNodeID)
	dynamicNodeStatus.SetDataDir(nCtx.NodeStatus().GetDataDir())
	dynamicNodeStatus.SetOutputDir(nCtx.NodeStatus().GetOutputDir())
	dynamicNodeStatus.SetParentTaskID(execID)
	id := nCtx.NodeID()
	dynamicNodeStatus.SetParentNodeID(&id)

	cacheHitStopWatch := d.metrics.CacheHit.Start(ctx)
	// Check if we have compiled the workflow before:
	// If there is a cached compiled Workflow, load and return it.
	if ok, err := f.CacheExists(ctx); err != nil {
		logger.Warnf(ctx, "Failed to call head on compiled workflow files. Error: %v", err)
		return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "Failed to do HEAD on compiled workflow files.")
	} else if ok {
		// It exists, load and return it
		workflowCacheContents, err := f.RetrieveCache(ctx)
		if err != nil {
			logger.Warnf(ctx, "Failed to load cached flyte workflow, this will cause the dynamic workflow to be recompiled. Error: %v", err)
			d.metrics.CacheError.Inc(ctx)
		} else {
			// We know for sure that futures file was generated. Lets read it
			djSpec, err := f.Read(ctx)
			if err != nil {
				return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "unable to read futures file, maybe corrupted")
			}

			err = setEphemeralNodeExecutionStatusAttributes(ctx, djSpec, nCtx, dynamicNodeStatus)
			if err != nil {
				return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "failed to set ephemeral node execution attributions")
			}

			newParentInfo, err := node_common.CreateParentInfo(nCtx.ExecutionContext().GetParentInfo(), nCtx.NodeID(), nCtx.CurrentAttempt())
			if err != nil {
				return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "failed to generate uniqueID")
			}

			compiledWf := workflowCacheContents.WorkflowCRD

			cacheHitStopWatch.Stop()

			return dynamicWorkflowContext{
				isDynamic:          true,
				subWorkflow:        compiledWf,
				subWorkflowClosure: workflowCacheContents.CompiledWorkflow,
				execContext:        executors.NewExecutionContext(nCtx.ExecutionContext(), compiledWf, compiledWf, newParentInfo, nCtx.ExecutionContext()),
				nodeLookup:         executors.NewNodeLookup(compiledWf, dynamicNodeStatus, compiledWf),
				dynamicJobSpecURI:  string(f.GetLoc()),
			}, nil
		}
	}
	d.metrics.CacheMiss.Inc(ctx)

	// We know for sure that futures file was generated. Lets read it
	djSpec, err := f.Read(ctx)
	if err != nil {
		return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "unable to read futures file, maybe corrupted")
	}

	closure, dynamicWf, err := d.buildDynamicWorkflow(ctx, nCtx, djSpec, dynamicNodeStatus)
	if err != nil {
		return dynamicWorkflowContext{}, err
	}

	if err := f.Cache(ctx, dynamicWf, closure); err != nil {
		logger.Errorf(ctx, "Failed to cache Dynamic workflow [%s]", err.Error())
	}

	// The current node would end up becoming the parent for the dynamic task nodes.
	// This is done to track the lineage. For level zero, the CreateParentInfo will return nil
	newParentInfo, err := node_common.CreateParentInfo(nCtx.ExecutionContext().GetParentInfo(), nCtx.NodeID(), nCtx.CurrentAttempt())
	if err != nil {
		return dynamicWorkflowContext{}, errors.Wrapf(utils.ErrorCodeSystem, err, "failed to generate uniqueID")
	}
	return dynamicWorkflowContext{
		isDynamic:          true,
		subWorkflow:        dynamicWf,
		subWorkflowClosure: closure,
		execContext:        executors.NewExecutionContext(nCtx.ExecutionContext(), dynamicWf, dynamicWf, newParentInfo, nCtx.ExecutionContext()),
		nodeLookup:         executors.NewNodeLookup(dynamicWf, dynamicNodeStatus, dynamicWf),
		dynamicJobSpecURI:  string(f.GetLoc()),
	}, nil
}

func (d dynamicNodeTaskNodeHandler) buildDynamicWorkflow(ctx context.Context, nCtx handler.NodeExecutionContext,
	djSpec *core.DynamicJobSpec, dynamicNodeStatus v1alpha1.ExecutableNodeStatus) (*core.CompiledWorkflowClosure, *v1alpha1.FlyteWorkflow, error) {
	wf, err := d.buildDynamicWorkflowTemplate(ctx, djSpec, nCtx, dynamicNodeStatus)
	if err != nil {
		return nil, nil, errors.Wrapf(utils.ErrorCodeSystem, err, "failed to build dynamic workflow template")
	}

	compiledTasks, err := compileTasks(ctx, djSpec.Tasks)
	if err != nil {
		return nil, nil, errors.Wrapf(utils.ErrorCodeUser, err, "failed to compile dynamic tasks")
	}

	// Get the requirements, that is, a list of all the task IDs and the launch plan IDs that will be called as part of this dynamic task.
	// The definition of these will need to be fetched from Admin (in order to get the interface).
	requirements, err := compiler.GetRequirements(wf, djSpec.Subworkflows)
	if err != nil {
		return nil, nil, errors.Wrapf(utils.ErrorCodeUser, err, "failed to Get requirements for subworkflows")
	}

	// This method handles user vs system errors internally
	launchPlanInterfaces, err := d.getLaunchPlanInterfaces(ctx, requirements.GetRequiredLaunchPlanIds())
	if err != nil {
		return nil, nil, err
	}

	// TODO: In addition to querying Admin for launch plans, we also need to get all the tasks that are missing from the dynamic job spec.
	// 	 	 The reason they might be missing is because if a user yields a task that is SdkTask.fetch'ed, it should not be included
	// 	     See https://github.com/flyteorg/flyte/issues/219 for more information.

	var closure *core.CompiledWorkflowClosure
	closure, err = compiler.CompileWorkflow(wf, djSpec.Subworkflows, compiledTasks, launchPlanInterfaces)
	if err != nil {
		return nil, nil, errors.Wrapf(utils.ErrorCodeUser, err, "malformed dynamic workflow")
	}

	dynamicWf, err := k8s.BuildFlyteWorkflow(closure, &core.LiteralMap{}, nil, "")
	if err != nil {
		return nil, nil, errors.Wrapf(utils.ErrorCodeSystem, err, "failed to build workflow")
	}

	return closure, dynamicWf, nil
}

func (d dynamicNodeTaskNodeHandler) progressDynamicWorkflow(ctx context.Context, execContext executors.ExecutionContext, dynamicWorkflow v1alpha1.ExecutableWorkflow, nl executors.NodeLookup,
	nCtx handler.NodeExecutionContext, prevState handler.DynamicNodeState) (handler.Transition, handler.DynamicNodeState, error) {

	if err := d.nodeExecutor.RecursiveNodeHandler(ctx, execContext, dynamicWorkflow, nl, dynamicWorkflow.StartNode()); err != nil {
		return handler.UnknownTransition, prevState, err
	}
	state, err := execContext.Wait()
	if err != nil {
		return handler.UnknownTransition, prevState, err
	}

	if state.HasFailed() || state.HasTimedOut() {
		// When the subworkflow either fails or times-out we need to handle failing
		if dynamicWorkflow.GetOnFailureNode() != nil {
			// TODO Once we migrate to closure node we need to handle subworkflow using the subworkflow handler
			logger.Errorf(ctx, "We do not support failure nodes in dynamic workflow today")
		}

		// As we do not support Failure Node, we can just return failure in this case
		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoDynamicRunning(nil)),
			handler.DynamicNodeState{
				Phase:              v1alpha1.DynamicNodePhaseFailing,
				Reason:             "Dynamic workflow failed",
				Error:              state.Err,
				IsFailurePermanent: state.HasFailed(),
			}, nil
	}

	if state.IsComplete() {
		var o *handler.OutputInfo
		// If the WF interface has outputs, validate that the outputs file was written.
		if outputBindings := dynamicWorkflow.GetOutputBindings(); len(outputBindings) > 0 {
			dynamicNodeStatus := nCtx.NodeStatus().GetNodeExecutionStatus(ctx, dynamicNodeID)
			endNodeStatus := dynamicNodeStatus.GetNodeExecutionStatus(ctx, v1alpha1.EndNodeID)
			if endNodeStatus == nil {
				return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoFailure(core.ExecutionError_SYSTEM, "MalformedDynamicWorkflow", "no end-node found in dynamic workflow", nil)),
					handler.DynamicNodeState{Phase: v1alpha1.DynamicNodePhaseFailing, Reason: "no end-node found in dynamic workflow"},
					nil
			}

			sourcePath := v1alpha1.GetOutputsFile(endNodeStatus.GetOutputDir())
			if metadata, err := nCtx.DataStore().Head(ctx, sourcePath); err == nil {
				if !metadata.Exists() {
					return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoRetryableFailure(core.ExecutionError_SYSTEM, "DynamicWorkflowOutputsNotFound", fmt.Sprintf(" is expected to produce outputs but no outputs file was written to %v.", sourcePath), nil)),
						handler.DynamicNodeState{Phase: v1alpha1.DynamicNodePhaseFailing, Reason: "DynamicWorkflow is expected to produce outputs but no outputs file was written"},
						nil
				}
			} else {
				return handler.UnknownTransition, prevState, err
			}

			destinationPath := v1alpha1.GetOutputsFile(nCtx.NodeStatus().GetOutputDir())
			if err := nCtx.DataStore().CopyRaw(ctx, sourcePath, destinationPath, storage.Options{}); err != nil {
				return handler.DoTransition(handler.TransitionTypeEphemeral,
						handler.PhaseInfoFailure(core.ExecutionError_SYSTEM, "OutputsNotFound",
							fmt.Sprintf("Failed to copy subworkflow outputs from [%v] to [%v]. Error: %s", sourcePath, destinationPath, err.Error()), nil),
					), handler.DynamicNodeState{Phase: v1alpha1.DynamicNodePhaseFailing, Reason: "Failed to copy subworkflow outputs"},
					nil
			}

			o = &handler.OutputInfo{OutputURI: destinationPath}
		}

		return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoSuccess(&handler.ExecutionInfo{
			OutputInfo: o,
		})), prevState, nil
	}

	if state.PartiallyComplete() {
		if err := nCtx.EnqueueOwnerFunc()(); err != nil {
			return handler.UnknownTransition, prevState, err
		}
	}

	return handler.DoTransition(handler.TransitionTypeEphemeral, handler.PhaseInfoDynamicRunning(nil)), prevState, nil
}

func (d dynamicNodeTaskNodeHandler) getLaunchPlanInterfaces(ctx context.Context, launchPlanIDs []compiler.LaunchPlanRefIdentifier) (
	[]common.InterfaceProvider, error) {

	var launchPlanInterfaces = make([]common.InterfaceProvider, len(launchPlanIDs))
	for idx, id := range launchPlanIDs {
		idVal := id
		lp, err := d.lpReader.GetLaunchPlan(ctx, &idVal)
		if err != nil {
			logger.Debugf(ctx, "Error fetching launch plan definition from admin")
			if launchplan.IsNotFound(err) || launchplan.IsUserError(err) {
				return nil, errors.Wrapf(utils.ErrorCodeUser, err, "incorrectly specified launchplan %s:%s:%s:%s",
					id.Project, id.Domain, id.Name, id.Version)
			}
			return nil, errors.Wrapf(utils.ErrorCodeSystem, err, "unable to retrieve launchplan information %s:%s:%s:%s",
				id.Project, id.Domain, id.Name, id.Version)
		}
		launchPlanInterfaces[idx] = compiler.NewLaunchPlanInterfaceProvider(*lp)
	}

	return launchPlanInterfaces, nil
}
