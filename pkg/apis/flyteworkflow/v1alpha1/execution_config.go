package v1alpha1

import (
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/admin"
	"k8s.io/apimachinery/pkg/api/resource"
)

// This contains an OutputLocationPrefix. When running against AWS, this should be something of the form
// s3://my-bucket, or s3://my-bucket/  A sharding string will automatically be appended to this prefix before
// handing off to plugins/tasks. Sharding behavior may change in the future.
// Background available at https://github.com/flyteorg/flyte/issues/211
type RawOutputDataConfig struct {
	*admin.RawOutputDataConfig `json:",inline"`
}

func (in *RawOutputDataConfig) DeepCopyInto(out *RawOutputDataConfig) {
	*out = *in
}

// This contains workflow-execution specifications and overrides.
type ExecutionConfig struct {
	// Maps individual task types to their alternate (non-default) plugin handlers by name.
	TaskPluginImpls map[string]TaskPluginOverride `json:"taskPluginImpls,omitempty"`
	// Can be used to control the number of parallel nodes to run within the workflow. This is useful to achieve fairness.
	MaxParallelism uint32 `json:"maxParallelism,omitempty"`
	// Defines execution behavior for processing nodes.
	RecoveryExecution WorkflowExecutionIdentifier `json:"recoveryExecution,omitempty"`
	// Defines the resource requests and limits specified for tasks run as part of this execution that ought to be
	// applied at execution time.
	TaskResources TaskResources `json:"taskResources,omitempty"`
}

type TaskPluginOverride struct {
	// +listType=atomic
	PluginIDs             []string                                   `json:"pluginIDs,omitempty"`
	MissingPluginBehavior admin.PluginOverride_MissingPluginBehavior `json:"missingPluginBehavior,omitempty"`
}

// Defines a set of configurable resources of different types that a task can request or apply as limits.
type TaskResourceSpec struct {
	CPU              resource.Quantity `json:"cpu,omitempty"`
	Memory           resource.Quantity `json:"memory,omitempty"`
	EphemeralStorage resource.Quantity `json:"ephemeralStorage,omitempty"`
	Storage          resource.Quantity `json:"storage,omitempty"`
	GPU              resource.Quantity `json:"gpu,omitempty"`
}

// Defines the complete closure of compute resources a task can request and apply as limits.
type TaskResources struct {
	// If the node where a task is running has enough of a resource available, a
	// container may use more resources than its request for that resource specifies.
	Requests TaskResourceSpec `json:"requests,omitempty"`
	// A hard limit, a task cannot consume resources greater than the limit specifies.
	Limits TaskResourceSpec `json:"limits,omitempty"`
}
