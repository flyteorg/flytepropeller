package extender


import (
	"github.com/lyft/flytepropeller/k8s-scheduler/pkg/controller/resourceRequest"
		extenderv1 "k8s.io/kube-scheduler/extender/v1"
)

// FlyteSchedulerExtender is a kubernetes scheduler extender responsible for flattening the
// resource requestHandler curve.
type FlyteSchedulerExtender struct {
	requestHandler resourceRequest.Handler
}

func (f *FlyteSchedulerExtender) Filter(args *extenderv1.ExtenderArgs) (*extenderv1.ExtenderFilterResult, error) {
	// TODO ssingh:
	//        Get the resource-requestHandler label from the pod
	//        if label doesn't exist
	//           then let the pod go
	//        else get the resource requestHandler name
	//           check if requestHandler is approved.
	return &extenderv1.ExtenderFilterResult{}, nil
}
