// Code generated by mockery v1.0.1. DO NOT EDIT.

package mocks

import (
	v1alpha1 "github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	mock "github.com/stretchr/testify/mock"
)

// MutableWorkflowNodeStatus is an autogenerated mock type for the MutableWorkflowNodeStatus type
type MutableWorkflowNodeStatus struct {
	mock.Mock
}

type MutableWorkflowNodeStatus_GetWorkflowNodePhase struct {
	*mock.Call
}

func (_m MutableWorkflowNodeStatus_GetWorkflowNodePhase) Return(_a0 v1alpha1.WorkflowNodePhase) *MutableWorkflowNodeStatus_GetWorkflowNodePhase {
	return &MutableWorkflowNodeStatus_GetWorkflowNodePhase{Call: _m.Call.Return(_a0)}
}

func (_m *MutableWorkflowNodeStatus) OnGetWorkflowNodePhase() *MutableWorkflowNodeStatus_GetWorkflowNodePhase {
	c := _m.On("GetWorkflowNodePhase")
	return &MutableWorkflowNodeStatus_GetWorkflowNodePhase{Call: c}
}

func (_m *MutableWorkflowNodeStatus) OnGetWorkflowNodePhaseMatch(matchers ...interface{}) *MutableWorkflowNodeStatus_GetWorkflowNodePhase {
	c := _m.On("GetWorkflowNodePhase", matchers...)
	return &MutableWorkflowNodeStatus_GetWorkflowNodePhase{Call: c}
}

// GetWorkflowNodePhase provides a mock function with given fields:
func (_m *MutableWorkflowNodeStatus) GetWorkflowNodePhase() v1alpha1.WorkflowNodePhase {
	ret := _m.Called()

	var r0 v1alpha1.WorkflowNodePhase
	if rf, ok := ret.Get(0).(func() v1alpha1.WorkflowNodePhase); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(v1alpha1.WorkflowNodePhase)
	}

	return r0
}

// SetWorkflowNodePhase provides a mock function with given fields: phase
func (_m *MutableWorkflowNodeStatus) SetWorkflowNodePhase(phase v1alpha1.WorkflowNodePhase) {
	_m.Called(phase)
}
