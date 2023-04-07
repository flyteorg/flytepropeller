package v1alpha1

import (
)

type ArrayNodeSpec struct {
	SubNodeSpec *NodeSpec 
	// TODO @hamersaw - fill out ArrayNodeSpec
}

func (a *ArrayNodeSpec) GetSubNodeSpec() *NodeSpec {
	return a.SubNodeSpec
}
