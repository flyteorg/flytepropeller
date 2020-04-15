package v1alpha1

import (
	"github.com/lyft/flytestdlib/storage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// FlyteJob: represents one Execution Workflow object
type FlyteJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	*NodeSpec         `json:"spec"`
	Inputs            *FlyteInputs                 `json:",inline"`
	ExecutionID       ExecutionID                  `json:"executionId"`
	Tasks             map[TaskID]*TaskSpec         `json:"tasks"`
	SubWorkflows      map[WorkflowID]*WorkflowSpec `json:"subWorkflows,omitempty"`
	// Defaults value of parameters to be used for nodes if not set by the node.
	NodeDefaults NodeDefaults `json:"node-defaults,omitempty"`
	// Specifies the time when the workflow has been accepted into the system.
	AcceptedAt *metav1.Time `json:"acceptedAt,omitempty"`
	// ServiceAccountName is the name of the ServiceAccount to use to run this pod.
	// More info: https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,8,opt,name=serviceAccountName"`
	// Status is the only mutable section in the workflow. It holds all the execution information
	Status JobStatus `json:"status,omitempty"`
	// non-Serialized fields
	DataReferenceConstructor storage.ReferenceConstructor `json:"-"`
}

type FlyteInputs struct {
	Inputs   *Inputs                `json:"inputs"`
	InputRef *storage.DataReference `json:"input-ref"`
}
