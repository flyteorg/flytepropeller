package compiler

import (
	"testing"

	"github.com/flyteorg/flyteidl/clients/go/coreutils"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/admin"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
)

var launchPlanIdentifier = core.Identifier{
	ResourceType: core.ResourceType_LAUNCH_PLAN,
	Project:      "project",
	Domain:       "domain",
	Name:         "name",
	Version:      "version",
}

var inputs = core.ParameterMap{
	Parameters: []*core.ParameterMapFieldEntry{
		{
			Key: "foo",
			Value: &core.Parameter{
				Var: &core.Variable{
					Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_STRING}},
				},
				Behavior: &core.Parameter_Default{
					Default: coreutils.MustMakeLiteral("foo-value"),
				},
			},
		},
	},
}
var outputs = core.VariableMap{
	Variables: []*core.VariableMapFieldEntry{
		{
			Key: "foo",
			Value: &core.Variable{
				Type: &core.LiteralType{Type: &core.LiteralType_Simple{Simple: core.SimpleType_STRING}},
			},
		},
	},
}

func getDummyLaunchPlan() admin.LaunchPlan {
	launchPlanClosure := admin.LaunchPlanClosure{
		ExpectedInputs:  &inputs,
		ExpectedOutputs: &outputs,
	}
	return admin.LaunchPlan{
		Id:      &launchPlanIdentifier,
		Spec:    nil,
		Closure: &launchPlanClosure,
	}
}

func TestGetId(t *testing.T) {
	launchPlan := getDummyLaunchPlan()
	provider := NewLaunchPlanInterfaceProvider(launchPlan)
	assert.Equal(t, &core.Identifier{ResourceType: 3, Project: "project", Domain: "domain", Name: "name", Version: "version"}, provider.GetID())
}

func TestGetExpectedInputs(t *testing.T) {
	launchPlan := getDummyLaunchPlan()
	provider := NewLaunchPlanInterfaceProvider(launchPlan)
	assert.Equal(t, 1, len((*provider.GetExpectedInputs()).Parameters))
	assert.Equal(t, "foo", (*provider.GetExpectedInputs()).Parameters[0].GetKey())
	assert.NotNil(t, (*provider.GetExpectedInputs()).Parameters[0].GetValue().Var.Type.GetSimple())
	assert.EqualValues(t, "STRING", (*provider.GetExpectedInputs()).Parameters[0].GetValue().Var.Type.GetSimple().String())
	assert.NotNil(t, (*provider.GetExpectedInputs()).Parameters[0].GetValue().GetDefault())
}

func TestGetExpectedOutputs(t *testing.T) {
	launchPlan := getDummyLaunchPlan()
	provider := NewLaunchPlanInterfaceProvider(launchPlan)
	assert.Equal(t, 1, len(outputs.Variables))
	assert.Equal(t, "foo", outputs.Variables[0].GetKey())
	assert.EqualValues(t, outputs.Variables[0].GetValue().GetType().GetType(),
		provider.GetExpectedOutputs().Variables[0].GetValue().GetType().GetType())
}
