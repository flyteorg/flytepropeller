package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/resource"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ResourceRequestList struct {
	metav1.TypeMeta                   `json:",inline"`
	metav1.ListMeta                   `json:"metadata"`
	Items           []ResourceRequest `json:"items"`
}

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true
type ResourceRequest struct {
	metav1.TypeMeta              `json:",inline"`
	metav1.ObjectMeta            `json:"metadata"`
	Spec   ResourceRequestSpec   `json:"spec"`
	Status ResourceRequestStatus `json:"status,omitempty"`
}

type ResourceRequestSpec struct {
	SubmittedAt   *metav1.Time      `json:"submittedAt,omitempty"`
	QueuingBudget *metav1.Duration  `json:"queuingBudget,omitempty"`
	Cpu           resource.Quantity `json:"Cpu"`
	Memory        resource.Quantity `json:"Memory"`
}

type ResourceRequestPhase string

func (p ResourceRequestPhase) VerboseString() string {
	phaseName := string(p)
	if p == RequestUnknown {
		phaseName = "Unknown"
	}
	return phaseName
}

const (
	RequestUnknown   ResourceRequestPhase = ""
	RequestKnown     ResourceRequestPhase = "Known"
	RequestAllocated ResourceRequestPhase = "Allocated"
)

var ResourceRequesPhases = []ResourceRequestPhase{
	RequestUnknown,
	RequestKnown,
	RequestAllocated,
}

type ResourceRequestStatus struct {
	Phase         ResourceRequestPhase `json:"-"`
	AcceptedAt    *metav1.Time         `json:"startedAt,omitempty"`
	LastUpdatedAt *metav1.Time         `json:"lastUpdatedAt,omitempty"`
	Reason        string               `json:"reason,omitempty"`
	// TODO add status of pods started in this reservation... And also under and over-provisioned
}