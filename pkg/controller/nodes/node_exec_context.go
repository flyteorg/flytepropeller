package nodes

import (
	"context"
	"fmt"
	"strconv"

	"github.com/lyft/flyteidl/clients/go/events"
	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytepropeller/pkg/controller/workflow"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/storage"
	"k8s.io/apimachinery/pkg/types"

	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
	"github.com/lyft/flyteplugins/go/tasks/pluginmachinery/ioutils"

	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/handler"
	"github.com/lyft/flytepropeller/pkg/utils"
)

const NodeIDLabel = "node-id"
const TaskNameLabel = "task-name"
const NodeInterruptibleLabel = "interruptible"

type nodeExecMetadata struct {
	v1alpha1.Meta
	nodeExecID         *core.NodeExecutionIdentifier
	interruptible      bool
	nodeLabels         map[string]string
	queueBudgetHandler workflow.QueueBudgetHandler
}

func (e nodeExecMetadata) GetNodeExecutionID() *core.NodeExecutionIdentifier {
	return e.nodeExecID
}

func (e nodeExecMetadata) GetK8sServiceAccount() string {
	return e.Meta.GetServiceAccountName()
}

func (e nodeExecMetadata) GetOwnerID() types.NamespacedName {
	return types.NamespacedName{Name: e.GetName(), Namespace: e.GetNamespace()}
}

func (e nodeExecMetadata) IsInterruptible() bool {
	return e.interruptible
}

func (e nodeExecMetadata) GetQueuingBudgetAllocator() workflow.QueueBudgetHandler {
	return e.queueBudgetHandler
}

func (e nodeExecMetadata) GetLabels() map[string]string {
	return e.nodeLabels
}

type nodeExecContext struct {
	store               *storage.DataStore
	tr                  handler.TaskReader
	md                  handler.NodeExecutionMetadata
	er                  events.TaskEventRecorder
	inputs              io.InputReader
	node                v1alpha1.ExecutableNode
	nodeStatus          v1alpha1.ExecutableNodeStatus
	maxDatasetSizeBytes int64
	nsm                 *nodeStateManager
	enqueueOwner        func() error
	rawOutputPrefix     storage.DataReference
	shardSelector       ioutils.ShardSelector
	nl                  executors.NodeLookup
	ic                  executors.ExecutionContext
}

func (e nodeExecContext) ExecutionContext() executors.ExecutionContext {
	return e.ic
}

func (e nodeExecContext) ContextualNodeLookup() executors.NodeLookup {
	return e.nl
}

func (e nodeExecContext) OutputShardSelector() ioutils.ShardSelector {
	return e.shardSelector
}

func (e nodeExecContext) RawOutputPrefix() storage.DataReference {
	return e.rawOutputPrefix
}

func (e nodeExecContext) EnqueueOwnerFunc() func() error {
	return e.enqueueOwner
}

func (e nodeExecContext) TaskReader() handler.TaskReader {
	return e.tr
}

func (e nodeExecContext) NodeStateReader() handler.NodeStateReader {
	return e.nsm
}

func (e nodeExecContext) NodeStateWriter() handler.NodeStateWriter {
	return e.nsm
}

func (e nodeExecContext) DataStore() *storage.DataStore {
	return e.store
}

func (e nodeExecContext) InputReader() io.InputReader {
	return e.inputs
}

func (e nodeExecContext) EventsRecorder() events.TaskEventRecorder {
	return e.er
}

func (e nodeExecContext) NodeID() v1alpha1.NodeID {
	return e.node.GetID()
}

func (e nodeExecContext) Node() v1alpha1.ExecutableNode {
	return e.node
}

func (e nodeExecContext) CurrentAttempt() uint32 {
	return e.nodeStatus.GetAttempts()
}

func (e nodeExecContext) NodeStatus() v1alpha1.ExecutableNodeStatus {
	return e.nodeStatus
}

func (e nodeExecContext) NodeExecutionMetadata() handler.NodeExecutionMetadata {
	return e.md
}

func (e nodeExecContext) MaxDatasetSizeBytes() int64 {
	return e.maxDatasetSizeBytes
}

func newNodeExecContext(ctx context.Context, store *storage.DataStore, execContext executors.ExecutionContext, nl executors.NodeLookup, node v1alpha1.ExecutableNode, nodeStatus v1alpha1.ExecutableNodeStatus, inputs io.InputReader, maxDatasetSize int64, er events.TaskEventRecorder, tr handler.TaskReader, nsm *nodeStateManager, enqueueOwner func() error, rawOutputPrefix storage.DataReference, outputShardSelector ioutils.ShardSelector) *nodeExecContext {

	// TODO ssingh: dont initialize it here, instead pass this down from caller
	queueBudgetHandler := workflow.NewDefaultQueueBudgetHandler(nil, nl)

	md := nodeExecMetadata{
		Meta: execContext,
		nodeExecID: &core.NodeExecutionIdentifier{
			NodeId:      node.GetID(),
			ExecutionId: execContext.GetExecutionID().WorkflowExecutionIdentifier,
		},
		queueBudgetHandler: queueBudgetHandler,
	}

	// Copy the wf labels before adding node specific labels.
	nodeLabels := make(map[string]string)
	for k, v := range execContext.GetLabels() {
		nodeLabels[k] = v
	}
	nodeLabels[NodeIDLabel] = utils.SanitizeLabelValue(node.GetID())
	if tr != nil && tr.GetTaskID() != nil {
		nodeLabels[TaskNameLabel] = utils.SanitizeLabelValue(tr.GetTaskID().Name)
	}

	schedulingParameters, err := queueBudgetHandler.GetNodeQueuingParameters(ctx, node.GetID())
	if err != nil {
		// TODO: return err
		logger.Error(ctx, err)
	}
	nodeLabels[NodeInterruptibleLabel] = strconv.FormatBool(schedulingParameters.IsInterruptible)
	md.nodeLabels = nodeLabels

	return &nodeExecContext{
		md:                  md,
		store:               store,
		node:                node,
		nodeStatus:          nodeStatus,
		inputs:              inputs,
		er:                  er,
		maxDatasetSizeBytes: maxDatasetSize,
		tr:                  tr,
		nsm:                 nsm,
		enqueueOwner:        enqueueOwner,
		rawOutputPrefix:     rawOutputPrefix,
		shardSelector:       outputShardSelector,
		nl:                  nl,
		ic:                  execContext,
	}
}

func (c *nodeExecutor) newNodeExecContextDefault(ctx context.Context, currentNodeID v1alpha1.NodeID, executionContext executors.ExecutionContext, nl executors.NodeLookup) (*nodeExecContext, error) {
	n, ok := nl.GetNode(currentNodeID)
	if !ok {
		return nil, fmt.Errorf("failed to find node with ID [%s] in execution [%s]", currentNodeID, executionContext.GetID())
	}

	var tr handler.TaskReader
	if n.GetKind() == v1alpha1.NodeKindTask {
		if n.GetTaskID() == nil {
			return nil, fmt.Errorf("bad state, no task-id defined for node [%s]", n.GetID())
		}
		tk, err := executionContext.GetTask(*n.GetTaskID())
		if err != nil {
			return nil, err
		}
		tr = taskReader{TaskTemplate: tk.CoreTask()}
	}

	workflowEnqueuer := func() error {
		c.enqueueWorkflow(executionContext.GetID())
		return nil
	}

	return newNodeExecContext(ctx, c.store, executionContext, nl, n, s,
		ioutils.NewCachedInputReader(
			ctx,
			ioutils.NewRemoteFileInputReader(
				ctx,
				c.store,
				ioutils.NewInputFilePaths(
					ctx,
					c.store,
					s.GetDataDir(),
				),
			),
		),
		c.maxDatasetSizeBytes,
		&taskEventRecorder{TaskEventRecorder: c.taskRecorder},
		tr,
		newNodeStateManager(ctx, s),
		workflowEnqueuer,
		// Eventually we want to replace this with per workflow sandboxes
		// https://github.com/lyft/flyte/issues/211
		c.defaultDataSandbox,
		c.shardSelector,
	), nil
}
