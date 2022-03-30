package controller

import (
	"testing"
	"time"

	"github.com/flyteorg/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIgnoreCompletedWorkflowsLabelSelector(t *testing.T) {
	s := IgnoreCompletedWorkflowsLabelSelector()
	assert.NotNil(t, s)
	assert.Empty(t, s.MatchLabels)
	assert.NotEmpty(t, s.MatchExpressions)
	r := s.MatchExpressions[0]
	assert.Equal(t, workflowTerminationStatusKey, r.Key)
	assert.Equal(t, v1.LabelSelectorOpNotIn, r.Operator)
	assert.Equal(t, []string{workflowTerminatedValue}, r.Values)
}

func TestCompletedWorkflowsLabelSelector(t *testing.T) {
	s := CompletedWorkflowsLabelSelector()
	assert.NotEmpty(t, s.MatchLabels)
	v, ok := s.MatchLabels[workflowTerminationStatusKey]
	assert.True(t, ok)
	assert.Equal(t, workflowTerminatedValue, v)
}

func TestHasCompletedLabel(t *testing.T) {

	n := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	t.Run("no-labels", func(t *testing.T) {

		w := &v1alpha1.FlyteWorkflow{}
		assert.Empty(t, w.Labels)
		assert.False(t, HasCompletedLabel(w))
		SetCompletedLabel(w, n)
		assert.NotEmpty(t, w.Labels)
		v, ok := w.Labels[workflowTerminationStatusKey]
		assert.True(t, ok)
		assert.Equal(t, workflowTerminatedValue, v)
		assert.True(t, HasCompletedLabel(w))
	})

	t.Run("existing-lables", func(t *testing.T) {
		w := &v1alpha1.FlyteWorkflow{
			ObjectMeta: v1.ObjectMeta{
				Labels: map[string]string{
					"x": "v",
				},
			},
		}
		assert.NotEmpty(t, w.Labels)
		assert.False(t, HasCompletedLabel(w))
		SetCompletedLabel(w, n)
		assert.NotEmpty(t, w.Labels)
		v, ok := w.Labels[workflowTerminationStatusKey]
		assert.True(t, ok)
		assert.Equal(t, workflowTerminatedValue, v)
		v, ok = w.Labels["x"]
		assert.True(t, ok)
		assert.Equal(t, "v", v)
		assert.True(t, HasCompletedLabel(w))
	})
}

func TestSetCompletedLabel(t *testing.T) {
	n := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	t.Run("no-labels", func(t *testing.T) {

		w := &v1alpha1.FlyteWorkflow{}
		assert.Empty(t, w.Labels)
		SetCompletedLabel(w, n)
		assert.NotEmpty(t, w.Labels)
		v, ok := w.Labels[workflowTerminationStatusKey]
		assert.True(t, ok)
		assert.Equal(t, workflowTerminatedValue, v)
	})

	t.Run("existing-lables", func(t *testing.T) {
		w := &v1alpha1.FlyteWorkflow{
			ObjectMeta: v1.ObjectMeta{
				Labels: map[string]string{
					"x": "v",
				},
			},
		}
		assert.NotEmpty(t, w.Labels)
		SetCompletedLabel(w, n)
		assert.NotEmpty(t, w.Labels)
		v, ok := w.Labels[workflowTerminationStatusKey]
		assert.True(t, ok)
		assert.Equal(t, workflowTerminatedValue, v)
		v, ok = w.Labels["x"]
		assert.True(t, ok)
		assert.Equal(t, "v", v)
	})

}

func TestCalculateHoursToDelete(t *testing.T) {
	assert.Equal(t, []string{
		"10.6", "10.5", "10.4", "10.3", "10.2", "10.1", "10.0",
	}, CalculateHoursToKeep(6, time.Date(2009, time.November, 10, 6, 0, 0, 0, time.UTC)))

	assert.Equal(t, []string{
		"1.3", "1.2", "1.1", "1.0", "30.23", "30.22", "30.21",
	}, CalculateHoursToKeep(6, time.Date(2009, time.October, 1, 3, 0, 0, 0, time.UTC)))

	assert.Equal(t, []string{
		"1.0", "31.23", "31.22", "31.21", "31.20", "31.19", "31.18",
	}, CalculateHoursToKeep(6, time.Date(2009, time.January, 1, 0, 0, 0, 0, time.UTC)))

	assert.Equal(t, []string{
		"10.22", "10.21", "10.20", "10.19", "10.18", "10.17", "10.16",
	}, CalculateHoursToKeep(6, time.Date(2009, time.November, 10, 22, 0, 0, 0, time.UTC)))

	assert.Equal(t, []string{
		"10.23", "10.22", "10.21", "10.20", "10.19", "10.18", "10.17",
	}, CalculateHoursToKeep(6, time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)))

	assert.Equal(t, []string{"10.20", "10.19"}, CalculateHoursToKeep(1, time.Date(2009, time.November, 10, 20, 0, 0, 0, time.UTC)))

	assert.Equal(t, []string{
		"10.12", "10.11", "10.10", "10.9", "10.8", "10.7", "10.6", "10.5", "10.4", "10.3", "10.2", "10.1", "10.0", "9.23", "9.22", "9.21", "9.20", "9.19", "9.18", "9.17", "9.16", "9.15", "9.14",
	}, CalculateHoursToKeep(22, time.Date(2009, time.November, 10, 12, 0, 0, 0, time.UTC)))
	assert.Equal(t, []string{
		"10.0", "9.23", "9.22", "9.21", "9.20", "9.19", "9.18", "9.17", "9.16", "9.15", "9.14", "9.13", "9.12", "9.11", "9.10", "9.9", "9.8", "9.7", "9.6", "9.5", "9.4", "9.3", "9.2",
	}, CalculateHoursToKeep(22, time.Date(2009, time.November, 10, 0, 0, 0, 0, time.UTC)))


	assert.Equal(t, []string{"30.12", "30.11"}, CalculateHoursToKeep(1, time.Date(2022, time.March, 30, 12, 10, 0, 0, time.UTC)))
}

func TestCompletedWorkflowsSelectorOutsideRetentionPeriod(t *testing.T) {
	n := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)
	s := CompletedWorkflowsSelectorOutsideRetentionPeriod(2, n)
	v, ok := s.MatchLabels[workflowTerminationStatusKey]
	assert.True(t, ok)
	assert.Equal(t, workflowTerminatedValue, v)
	assert.NotEmpty(t, s.MatchExpressions)
	r := s.MatchExpressions[0]
	assert.Equal(t, CompletedTimeKey, r.Key)
	assert.Equal(t, v1.LabelSelectorOpNotIn, r.Operator)
	assert.Equal(t, 3, len(r.Values))
	assert.Equal(t, []string{
		"10.23", "10.22", "10.21",
	}, r.Values)
}
