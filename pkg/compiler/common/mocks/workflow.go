// Code generated by mockery v1.0.1. DO NOT EDIT.

package mocks

import (
	core "github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	common "github.com/lyft/flytepropeller/pkg/compiler/common"

	mock "github.com/stretchr/testify/mock"
)

// Workflow is an autogenerated mock type for the Workflow type
type Workflow struct {
	mock.Mock
}

type Workflow_GetCompiledSubWorkflow struct {
	*mock.Call
}

func (_m Workflow_GetCompiledSubWorkflow) Return(wf *core.CompiledWorkflow, found bool) *Workflow_GetCompiledSubWorkflow {
	return &Workflow_GetCompiledSubWorkflow{Call: _m.Call.Return(wf, found)}
}

func (_m *Workflow) OnGetCompiledSubWorkflow(id core.Identifier) *Workflow_GetCompiledSubWorkflow {
	c := _m.On("GetCompiledSubWorkflow", id)
	return &Workflow_GetCompiledSubWorkflow{Call: c}
}

func (_m *Workflow) OnGetCompiledSubWorkflowMatch(matchers ...interface{}) *Workflow_GetCompiledSubWorkflow {
	c := _m.On("GetCompiledSubWorkflow", matchers...)
	return &Workflow_GetCompiledSubWorkflow{Call: c}
}

// GetCompiledSubWorkflow provides a mock function with given fields: id
func (_m *Workflow) GetCompiledSubWorkflow(id core.Identifier) (*core.CompiledWorkflow, bool) {
	ret := _m.Called(id)

	var r0 *core.CompiledWorkflow
	if rf, ok := ret.Get(0).(func(core.Identifier) *core.CompiledWorkflow); ok {
		r0 = rf(id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*core.CompiledWorkflow)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(core.Identifier) bool); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

type Workflow_GetCoreWorkflow struct {
	*mock.Call
}

func (_m Workflow_GetCoreWorkflow) Return(_a0 *core.CompiledWorkflow) *Workflow_GetCoreWorkflow {
	return &Workflow_GetCoreWorkflow{Call: _m.Call.Return(_a0)}
}

func (_m *Workflow) OnGetCoreWorkflow() *Workflow_GetCoreWorkflow {
	c := _m.On("GetCoreWorkflow")
	return &Workflow_GetCoreWorkflow{Call: c}
}

func (_m *Workflow) OnGetCoreWorkflowMatch(matchers ...interface{}) *Workflow_GetCoreWorkflow {
	c := _m.On("GetCoreWorkflow", matchers...)
	return &Workflow_GetCoreWorkflow{Call: c}
}

// GetCoreWorkflow provides a mock function with given fields:
func (_m *Workflow) GetCoreWorkflow() *core.CompiledWorkflow {
	ret := _m.Called()

	var r0 *core.CompiledWorkflow
	if rf, ok := ret.Get(0).(func() *core.CompiledWorkflow); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*core.CompiledWorkflow)
		}
	}

	return r0
}

type Workflow_GetDownstreamNodes struct {
	*mock.Call
}

func (_m Workflow_GetDownstreamNodes) Return(_a0 common.StringAdjacencyList) *Workflow_GetDownstreamNodes {
	return &Workflow_GetDownstreamNodes{Call: _m.Call.Return(_a0)}
}

func (_m *Workflow) OnGetDownstreamNodes() *Workflow_GetDownstreamNodes {
	c := _m.On("GetDownstreamNodes")
	return &Workflow_GetDownstreamNodes{Call: c}
}

func (_m *Workflow) OnGetDownstreamNodesMatch(matchers ...interface{}) *Workflow_GetDownstreamNodes {
	c := _m.On("GetDownstreamNodes", matchers...)
	return &Workflow_GetDownstreamNodes{Call: c}
}

// GetDownstreamNodes provides a mock function with given fields:
func (_m *Workflow) GetDownstreamNodes() common.StringAdjacencyList {
	ret := _m.Called()

	var r0 common.StringAdjacencyList
	if rf, ok := ret.Get(0).(func() common.StringAdjacencyList); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.StringAdjacencyList)
		}
	}

	return r0
}

type Workflow_GetFailureNode struct {
	*mock.Call
}

func (_m Workflow_GetFailureNode) Return(_a0 common.Node) *Workflow_GetFailureNode {
	return &Workflow_GetFailureNode{Call: _m.Call.Return(_a0)}
}

func (_m *Workflow) OnGetFailureNode() *Workflow_GetFailureNode {
	c := _m.On("GetFailureNode")
	return &Workflow_GetFailureNode{Call: c}
}

func (_m *Workflow) OnGetFailureNodeMatch(matchers ...interface{}) *Workflow_GetFailureNode {
	c := _m.On("GetFailureNode", matchers...)
	return &Workflow_GetFailureNode{Call: c}
}

// GetFailureNode provides a mock function with given fields:
func (_m *Workflow) GetFailureNode() common.Node {
	ret := _m.Called()

	var r0 common.Node
	if rf, ok := ret.Get(0).(func() common.Node); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.Node)
		}
	}

	return r0
}

type Workflow_GetLaunchPlan struct {
	*mock.Call
}

func (_m Workflow_GetLaunchPlan) Return(wf common.InterfaceProvider, found bool) *Workflow_GetLaunchPlan {
	return &Workflow_GetLaunchPlan{Call: _m.Call.Return(wf, found)}
}

func (_m *Workflow) OnGetLaunchPlan(id core.Identifier) *Workflow_GetLaunchPlan {
	c := _m.On("GetLaunchPlan", id)
	return &Workflow_GetLaunchPlan{Call: c}
}

func (_m *Workflow) OnGetLaunchPlanMatch(matchers ...interface{}) *Workflow_GetLaunchPlan {
	c := _m.On("GetLaunchPlan", matchers...)
	return &Workflow_GetLaunchPlan{Call: c}
}

// GetLaunchPlan provides a mock function with given fields: id
func (_m *Workflow) GetLaunchPlan(id core.Identifier) (common.InterfaceProvider, bool) {
	ret := _m.Called(id)

	var r0 common.InterfaceProvider
	if rf, ok := ret.Get(0).(func(core.Identifier) common.InterfaceProvider); ok {
		r0 = rf(id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.InterfaceProvider)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(core.Identifier) bool); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

type Workflow_GetNode struct {
	*mock.Call
}

func (_m Workflow_GetNode) Return(node common.NodeBuilder, found bool) *Workflow_GetNode {
	return &Workflow_GetNode{Call: _m.Call.Return(node, found)}
}

func (_m *Workflow) OnGetNode(id string) *Workflow_GetNode {
	c := _m.On("GetNode", id)
	return &Workflow_GetNode{Call: c}
}

func (_m *Workflow) OnGetNodeMatch(matchers ...interface{}) *Workflow_GetNode {
	c := _m.On("GetNode", matchers...)
	return &Workflow_GetNode{Call: c}
}

// GetNode provides a mock function with given fields: id
func (_m *Workflow) GetNode(id string) (common.NodeBuilder, bool) {
	ret := _m.Called(id)

	var r0 common.NodeBuilder
	if rf, ok := ret.Get(0).(func(string) common.NodeBuilder); ok {
		r0 = rf(id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.NodeBuilder)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(string) bool); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

type Workflow_GetNodes struct {
	*mock.Call
}

func (_m Workflow_GetNodes) Return(_a0 common.NodeIndex) *Workflow_GetNodes {
	return &Workflow_GetNodes{Call: _m.Call.Return(_a0)}
}

func (_m *Workflow) OnGetNodes() *Workflow_GetNodes {
	c := _m.On("GetNodes")
	return &Workflow_GetNodes{Call: c}
}

func (_m *Workflow) OnGetNodesMatch(matchers ...interface{}) *Workflow_GetNodes {
	c := _m.On("GetNodes", matchers...)
	return &Workflow_GetNodes{Call: c}
}

// GetNodes provides a mock function with given fields:
func (_m *Workflow) GetNodes() common.NodeIndex {
	ret := _m.Called()

	var r0 common.NodeIndex
	if rf, ok := ret.Get(0).(func() common.NodeIndex); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.NodeIndex)
		}
	}

	return r0
}

type Workflow_GetSubWorkflow struct {
	*mock.Call
}

func (_m Workflow_GetSubWorkflow) Return(wf *core.CompiledWorkflow, found bool) *Workflow_GetSubWorkflow {
	return &Workflow_GetSubWorkflow{Call: _m.Call.Return(wf, found)}
}

func (_m *Workflow) OnGetSubWorkflow(id core.Identifier) *Workflow_GetSubWorkflow {
	c := _m.On("GetSubWorkflow", id)
	return &Workflow_GetSubWorkflow{Call: c}
}

func (_m *Workflow) OnGetSubWorkflowMatch(matchers ...interface{}) *Workflow_GetSubWorkflow {
	c := _m.On("GetSubWorkflow", matchers...)
	return &Workflow_GetSubWorkflow{Call: c}
}

// GetSubWorkflow provides a mock function with given fields: id
func (_m *Workflow) GetSubWorkflow(id core.Identifier) (*core.CompiledWorkflow, bool) {
	ret := _m.Called(id)

	var r0 *core.CompiledWorkflow
	if rf, ok := ret.Get(0).(func(core.Identifier) *core.CompiledWorkflow); ok {
		r0 = rf(id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*core.CompiledWorkflow)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(core.Identifier) bool); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

type Workflow_GetTask struct {
	*mock.Call
}

func (_m Workflow_GetTask) Return(task common.Task, found bool) *Workflow_GetTask {
	return &Workflow_GetTask{Call: _m.Call.Return(task, found)}
}

func (_m *Workflow) OnGetTask(id core.Identifier) *Workflow_GetTask {
	c := _m.On("GetTask", id)
	return &Workflow_GetTask{Call: c}
}

func (_m *Workflow) OnGetTaskMatch(matchers ...interface{}) *Workflow_GetTask {
	c := _m.On("GetTask", matchers...)
	return &Workflow_GetTask{Call: c}
}

// GetTask provides a mock function with given fields: id
func (_m *Workflow) GetTask(id core.Identifier) (common.Task, bool) {
	ret := _m.Called(id)

	var r0 common.Task
	if rf, ok := ret.Get(0).(func(core.Identifier) common.Task); ok {
		r0 = rf(id)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.Task)
		}
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(core.Identifier) bool); ok {
		r1 = rf(id)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

type Workflow_GetTasks struct {
	*mock.Call
}

func (_m Workflow_GetTasks) Return(_a0 common.TaskIndex) *Workflow_GetTasks {
	return &Workflow_GetTasks{Call: _m.Call.Return(_a0)}
}

func (_m *Workflow) OnGetTasks() *Workflow_GetTasks {
	c := _m.On("GetTasks")
	return &Workflow_GetTasks{Call: c}
}

func (_m *Workflow) OnGetTasksMatch(matchers ...interface{}) *Workflow_GetTasks {
	c := _m.On("GetTasks", matchers...)
	return &Workflow_GetTasks{Call: c}
}

// GetTasks provides a mock function with given fields:
func (_m *Workflow) GetTasks() common.TaskIndex {
	ret := _m.Called()

	var r0 common.TaskIndex
	if rf, ok := ret.Get(0).(func() common.TaskIndex); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.TaskIndex)
		}
	}

	return r0
}

type Workflow_GetUpstreamNodes struct {
	*mock.Call
}

func (_m Workflow_GetUpstreamNodes) Return(_a0 common.StringAdjacencyList) *Workflow_GetUpstreamNodes {
	return &Workflow_GetUpstreamNodes{Call: _m.Call.Return(_a0)}
}

func (_m *Workflow) OnGetUpstreamNodes() *Workflow_GetUpstreamNodes {
	c := _m.On("GetUpstreamNodes")
	return &Workflow_GetUpstreamNodes{Call: c}
}

func (_m *Workflow) OnGetUpstreamNodesMatch(matchers ...interface{}) *Workflow_GetUpstreamNodes {
	c := _m.On("GetUpstreamNodes", matchers...)
	return &Workflow_GetUpstreamNodes{Call: c}
}

// GetUpstreamNodes provides a mock function with given fields:
func (_m *Workflow) GetUpstreamNodes() common.StringAdjacencyList {
	ret := _m.Called()

	var r0 common.StringAdjacencyList
	if rf, ok := ret.Get(0).(func() common.StringAdjacencyList); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(common.StringAdjacencyList)
		}
	}

	return r0
}
