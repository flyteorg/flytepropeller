package validators

import (
	"fmt"

	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/golang/protobuf/proto"
	"k8s.io/apimachinery/pkg/util/sets"
)

func containsBindingByVariableName(bindings []*core.Binding, name string) (found bool) {
	for _, b := range bindings {
		if b.Var == name {
			return true
		}
	}

	return false
}

func findVariableByName(vars *core.VariableMap, name string) (variable *core.Variable, found bool) {
	if vars == nil || vars.Variables == nil {
		return nil, false
	}

	variable, found = vars.Variables[name]
	return
}

func buildVariablesIndex(params *core.VariableMap) (map[string]*core.Variable, sets.String) {
	paramMap := make(map[string]*core.Variable, len(params.Variables))
	paramSet := sets.NewString()
	for paramName, param := range params.Variables {
		paramMap[paramName] = param
		paramSet.Insert(paramName)
	}

	return paramMap, paramSet
}

func filterVariables(vars *core.VariableMap, varNames sets.String) *core.VariableMap {
	res := &core.VariableMap{
		Variables: make(map[string]*core.Variable, len(varNames)),
	}

	for paramName, param := range vars.Variables {
		if varNames.Has(paramName) {
			res.Variables[paramName] = param
		}
	}

	return res
}

func withVariableName(param *core.Variable) (newParam *core.Variable, ok bool) {
	if raw, err := proto.Marshal(param); err == nil {
		newParam = &core.Variable{}
		if err = proto.Unmarshal(raw, newParam); err == nil {
			ok = true
		}
	}

	return
}

func UnionDistinctVariableMaps(m1, m2 map[string]*core.Variable) (map[string]*core.Variable, error) {
	res := make(map[string]*core.Variable, len(m1)+len(m2))
	for k, v := range m1 {
		res[k] = v
	}

	for k, v := range m2 {
		if existingV, exists := res[k]; exists {
			if v.Type.String() != existingV.Type.String() {
				return nil, fmt.Errorf("key already exists with a different type. %v has type [%v] on one side "+
					"and type [%v] on the other", k, existingV.Type.String(), v.Type.String())
			}
		}

		res[k] = v
	}

	return res, nil
}
