package k8s

import (
	"math"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/golang/protobuf/ptypes"
)

func refInt(i int) *int {
	return &i
}

func refStr(s string) *string {
	return &s
}

func computeRetryStrategy(n *core.Node, t *core.TaskTemplate) *v1alpha1.RetryStrategy {
	if n.GetMetadata() != nil && n.GetMetadata().GetRetries() != nil {
		return &v1alpha1.RetryStrategy{
			MinAttempts: refInt(int(n.GetMetadata().GetRetries().Retries + 1)),
		}
	}

	if t != nil && t.GetMetadata() != nil && t.GetMetadata().GetRetries() != nil {
		return &v1alpha1.RetryStrategy{
			MinAttempts: refInt(int(t.GetMetadata().GetRetries().Retries + 1)),
		}
	}

	return nil
}

func computeDeadline(n *core.Node) (*v1.Duration, error) {
	var deadline *v1.Duration
	if n.GetMetadata() != nil && n.GetMetadata().GetTimeout() != nil {
		duration, err := ptypes.Duration(n.GetMetadata().GetTimeout())
		if err != nil {
			return nil, err
		}
		deadline = &v1.Duration{
			Duration: duration,
		}
	}
	return deadline, nil
}

func getResources(task *core.TaskTemplate) *core.Resources {
	if task == nil {
		return nil
	}

	if task.GetContainer() == nil {
		return nil
	}

	return task.GetContainer().Resources
}

func toAliasValueArray(aliases []*core.Alias) []v1alpha1.Alias {
	if aliases == nil {
		return nil
	}

	res := make([]v1alpha1.Alias, 0, len(aliases))
	for _, alias := range aliases {
		res = append(res, v1alpha1.Alias{Alias: *alias})
	}

	return res
}

func toBindingValueArray(bindings []*core.Binding) []*v1alpha1.Binding {
	if bindings == nil {
		return nil
	}

	res := make([]*v1alpha1.Binding, 0, len(bindings))
	for _, binding := range bindings {
		res = append(res, &v1alpha1.Binding{Binding: binding})
	}

	return res
}

func minInt(i, j int) int {
	return int(math.Min(float64(i), float64(j)))
}