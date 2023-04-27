package v1alpha1

import (
)

type ArrayNodeSpec struct {
	SubNodeSpec *NodeSpec 
	Parallelism uint32
	// TODO @hamersaw - fill out ArrayNodeSpec
}

func (a *ArrayNodeSpec) GetSubNodeSpec() *NodeSpec {
	return a.SubNodeSpec
}

func (a *ArrayNodeSpec) GetParallelism() uint32 {
	return a.Parallelism
}
