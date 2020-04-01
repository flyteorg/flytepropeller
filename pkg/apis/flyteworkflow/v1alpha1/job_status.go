package v1alpha1

import (
	"github.com/lyft/flytestdlib/storage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type JobPhase int8

type JobStatus struct {
	Phase           JobPhase      `json:"phase"`
	StartedAt       *metav1.Time  `json:"startedAt,omitempty"`
	StoppedAt       *metav1.Time  `json:"stoppedAt,omitempty"`
	LastUpdatedAt   *metav1.Time  `json:"lastUpdatedAt,omitempty"`
	Message         string        `json:"message,omitempty"`
	DataDir         DataReference `json:"dataDir,omitempty"`
	OutputReference DataReference `json:"outputRef,omitempty"`

	// Status of the node being executed
	NodeStatus *NodeStatus `json:"nodeStatus,omitempty"`

	// Number of Attempts completed with rounds resulting in error. this is used to cap out poison pill workflows
	// that spin in an error loop. The value should be set at the global level and will be enforced. At the end of
	// the retries the workflow will fail
	FailedAttempts uint32 `json:"failedAttempts,omitempty"`

	// non-Serialized fields
	DataReferenceConstructor storage.ReferenceConstructor `json:"-"`
}
