// This package converts the output of the compiler into a K8s resource for propeller to execute.
package k8s

import (
	"fmt"
	"strings"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/compiler/common"
	"github.com/lyft/flytepropeller/pkg/compiler/errors"
	"github.com/lyft/flytepropeller/pkg/utils"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const ExecutionIDLabel = "execution-id"
const WorkflowNameLabel = "workflow-name"

func requiresInputs(w *core.WorkflowTemplate) bool {
	if w == nil || w.GetInterface() == nil || w.GetInterface().GetInputs() == nil ||
		w.GetInterface().GetInputs().Variables == nil {

		return false
	}

	return len(w.GetInterface().GetInputs().Variables) > 0
}

// Note: Update WorkflowNameFromID for any change made to WorkflowIDAsString
func WorkflowIDAsString(id *core.Identifier) string {
	b := strings.Builder{}
	_, err := b.WriteString(id.Project)
	if err != nil {
		return ""
	}

	_, err = b.WriteRune(':')
	if err != nil {
		return ""
	}

	_, err = b.WriteString(id.Domain)
	if err != nil {
		return ""
	}

	_, err = b.WriteRune(':')
	if err != nil {
		return ""
	}

	_, err = b.WriteString(id.Name)
	if err != nil {
		return ""
	}

	return b.String()
}

func WorkflowNameFromID(id string) string {
	tokens := strings.Split(id, ":")
	if len(tokens) != 3 {
		return ""
	}
	return tokens[2]
}

func buildFlyteWorkflowSpec(wf *core.CompiledWorkflow, tasks []*core.CompiledTask, errs errors.CompileErrors) (
	spec *v1alpha1.WorkflowSpec) {
	var failureN *v1alpha1.NodeSpec
	if n := wf.Template.GetFailureNode(); n != nil {
		failureN, _ = buildNodeSpec(n, tasks, errs.NewScope())
	}

	nodes, _ := buildNodes(wf.Template.GetNodes(), tasks, errs.NewScope())

	if errs.HasErrors() {
		return nil
	}

	outputBindings := make([]*v1alpha1.Binding, 0, len(wf.Template.Outputs))
	for _, b := range wf.Template.Outputs {
		outputBindings = append(outputBindings, &v1alpha1.Binding{
			Binding: b,
		})
	}

	var outputs *v1alpha1.OutputVarMap
	if wf.Template.GetInterface() != nil {
		outputs = &v1alpha1.OutputVarMap{VariableMap: wf.Template.GetInterface().Outputs}
	} else {
		outputs = &v1alpha1.OutputVarMap{VariableMap: &core.VariableMap{}}
	}

	return &v1alpha1.WorkflowSpec{
		ID:             WorkflowIDAsString(wf.Template.Id),
		OnFailure:      failureN,
		Nodes:          nodes,
		Connections:    buildConnections(wf),
		Outputs:        outputs,
		OutputBindings: outputBindings,
	}
}

func withSeparatorIfNotEmpty(value string) string {
	if len(value) > 0 {
		return fmt.Sprintf("%v-", value)
	}

	return ""
}

func generateName(wfID *core.Identifier, execID *core.WorkflowExecutionIdentifier) (
	name string, generateName string, label string, err error) {

	if execID != nil {
		return execID.Name, "", execID.Name, nil
	} else if wfID != nil {
		wid := fmt.Sprintf("%v%v%v",
			withSeparatorIfNotEmpty(wfID.Project),
			withSeparatorIfNotEmpty(wfID.Domain),
			wfID.Name,
		)

		// TODO: this is a hack until we figure out how to restrict generated names. K8s has a limitation of 63 chars
		wid = wid[:minInt(32, len(wid))]
		return "", fmt.Sprintf("%v-", wid), wid, nil
	} else {
		return "", "", "", fmt.Errorf("expected param not set. wfID or execID must be non-nil values")
	}
}

// Builds v1alpha1.FlyteWorkflow resource. Returned error, if not nil, is of type errors.CompilerErrors.
func BuildFlyteWorkflow(wfClosure *core.CompiledWorkflowClosure, inputs *core.LiteralMap,
	executionID *core.WorkflowExecutionIdentifier, namespace string) (*v1alpha1.FlyteWorkflow, error) {

	errs := errors.NewCompileErrors()
	if wfClosure == nil {
		errs.Collect(errors.NewValueRequiredErr("root", "wfClosure"))
		return nil, errs
	}

	primarySpec := buildFlyteWorkflowSpec(wfClosure.Primary, wfClosure.Tasks, errs.NewScope())
	subwfs := make(map[v1alpha1.WorkflowID]*v1alpha1.WorkflowSpec, len(wfClosure.SubWorkflows))
	for _, subWf := range wfClosure.SubWorkflows {
		spec := buildFlyteWorkflowSpec(wfClosure.Primary, wfClosure.Tasks, errs.NewScope())
		subwfs[subWf.Template.Id.String()] = spec
	}

	wf := wfClosure.Primary.Template
	tasks := wfClosure.Tasks
	// Fill in inputs in the start node.
	if inputs != nil {
		if ok := validateInputs(common.StartNodeID, wf.GetInterface(), *inputs, errs.NewScope()); !ok {
			return nil, errs
		}
	} else if requiresInputs(wf) {
		errs.Collect(errors.NewValueRequiredErr("root", "inputs"))
		return nil, errs
	}

	obj := &v1alpha1.FlyteWorkflow{
		TypeMeta: v1.TypeMeta{
			Kind:       v1alpha1.FlyteWorkflowKind,
			APIVersion: v1alpha1.SchemeGroupVersion.String(),
		},
		ObjectMeta: v1.ObjectMeta{
			Namespace: namespace,
			Labels:    map[string]string{},
		},
		Inputs:       &v1alpha1.Inputs{LiteralMap: inputs},
		WorkflowSpec: primarySpec,
		SubWorkflows: subwfs,
		Tasks:        buildTasks(tasks, errs.NewScope()),
	}

	var err error
	obj.ObjectMeta.Name, obj.ObjectMeta.GenerateName, obj.ObjectMeta.Labels[ExecutionIDLabel], err =
		generateName(wf.GetId(), executionID)

	if err != nil {
		errs.Collect(errors.NewWorkflowBuildError(err))
	}
	obj.ObjectMeta.Labels[WorkflowNameLabel] = utils.SanitizeLabelValue(WorkflowNameFromID(primarySpec.ID))

	if obj.Nodes == nil || obj.Connections.DownstreamEdges == nil {
		// If we come here, we'd better have an error generated earlier. Otherwise, add one to make sure build fails.
		if !errs.HasErrors() {
			errs.Collect(errors.NewWorkflowBuildError(fmt.Errorf("failed to build workflow for unknown reason." +
				" Make sure to pass this workflow through the compiler first")))
		}
	} else if startingNodes, err := obj.FromNode(v1alpha1.StartNodeID); err == nil && len(startingNodes) == 0 {
		errs.Collect(errors.NewWorkflowHasNoEntryNodeErr(wf.GetId().String()))
	} else if err != nil {
		errs.Collect(errors.NewWorkflowBuildError(err))
	}

	if errs.HasErrors() {
		return nil, errs
	}

	return obj, nil
}

func toMapOfLists(connections map[string]*core.ConnectionSet_IdList) map[string][]string {
	res := make(map[string][]string, len(connections))
	for key, val := range connections {
		res[key] = val.Ids
	}

	return res
}

func buildConnections(w *core.CompiledWorkflow) v1alpha1.Connections {
	res := v1alpha1.Connections{}
	res.DownstreamEdges = toMapOfLists(w.GetConnections().GetDownstream())
	res.UpstreamEdges = toMapOfLists(w.GetConnections().GetUpstream())
	return res
}
