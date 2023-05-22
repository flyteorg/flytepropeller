// Code generated by mockery v1.0.1. DO NOT EDIT.

package mocks

import (
	storage "github.com/flyteorg/flytestdlib/storage"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// ExecutableTaskNodeStatus is an autogenerated mock type for the ExecutableTaskNodeStatus type
type ExecutableTaskNodeStatus struct {
	mock.Mock
}

type ExecutableTaskNodeStatus_GetBarrierClockTick struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetBarrierClockTick) Return(_a0 uint32) *ExecutableTaskNodeStatus_GetBarrierClockTick {
	return &ExecutableTaskNodeStatus_GetBarrierClockTick{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetBarrierClockTick() *ExecutableTaskNodeStatus_GetBarrierClockTick {
	c_call := _m.On("GetBarrierClockTick")
	return &ExecutableTaskNodeStatus_GetBarrierClockTick{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetBarrierClockTickMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetBarrierClockTick {
	c_call := _m.On("GetBarrierClockTick", matchers...)
	return &ExecutableTaskNodeStatus_GetBarrierClockTick{Call: c_call}
}

// GetBarrierClockTick provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetBarrierClockTick() uint32 {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	return r0
}

type ExecutableTaskNodeStatus_GetCleanupOnFailure struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetCleanupOnFailure) Return(_a0 bool) *ExecutableTaskNodeStatus_GetCleanupOnFailure {
	return &ExecutableTaskNodeStatus_GetCleanupOnFailure{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetCleanupOnFailure() *ExecutableTaskNodeStatus_GetCleanupOnFailure {
	c_call := _m.On("GetCleanupOnFailure")
	return &ExecutableTaskNodeStatus_GetCleanupOnFailure{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetCleanupOnFailureMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetCleanupOnFailure {
	c_call := _m.On("GetCleanupOnFailure", matchers...)
	return &ExecutableTaskNodeStatus_GetCleanupOnFailure{Call: c_call}
}

// GetCleanupOnFailure provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetCleanupOnFailure() bool {
	ret := _m.Called()

	var r0 bool
	if rf, ok := ret.Get(0).(func() bool); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(bool)
	}

	return r0
}

type ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt) Return(_a0 time.Time) *ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt {
	return &ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetLastPhaseUpdatedAt() *ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt {
	c_call := _m.On("GetLastPhaseUpdatedAt")
	return &ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetLastPhaseUpdatedAtMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt {
	c_call := _m.On("GetLastPhaseUpdatedAt", matchers...)
	return &ExecutableTaskNodeStatus_GetLastPhaseUpdatedAt{Call: c_call}
}

// GetLastPhaseUpdatedAt provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetLastPhaseUpdatedAt() time.Time {
	ret := _m.Called()

	var r0 time.Time
	if rf, ok := ret.Get(0).(func() time.Time); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(time.Time)
	}

	return r0
}

type ExecutableTaskNodeStatus_GetPhase struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetPhase) Return(_a0 int) *ExecutableTaskNodeStatus_GetPhase {
	return &ExecutableTaskNodeStatus_GetPhase{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetPhase() *ExecutableTaskNodeStatus_GetPhase {
	c_call := _m.On("GetPhase")
	return &ExecutableTaskNodeStatus_GetPhase{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetPhaseMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetPhase {
	c_call := _m.On("GetPhase", matchers...)
	return &ExecutableTaskNodeStatus_GetPhase{Call: c_call}
}

// GetPhase provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetPhase() int {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

type ExecutableTaskNodeStatus_GetPhaseVersion struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetPhaseVersion) Return(_a0 uint32) *ExecutableTaskNodeStatus_GetPhaseVersion {
	return &ExecutableTaskNodeStatus_GetPhaseVersion{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetPhaseVersion() *ExecutableTaskNodeStatus_GetPhaseVersion {
	c_call := _m.On("GetPhaseVersion")
	return &ExecutableTaskNodeStatus_GetPhaseVersion{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetPhaseVersionMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetPhaseVersion {
	c_call := _m.On("GetPhaseVersion", matchers...)
	return &ExecutableTaskNodeStatus_GetPhaseVersion{Call: c_call}
}

// GetPhaseVersion provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetPhaseVersion() uint32 {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	return r0
}

type ExecutableTaskNodeStatus_GetPluginState struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetPluginState) Return(_a0 []byte) *ExecutableTaskNodeStatus_GetPluginState {
	return &ExecutableTaskNodeStatus_GetPluginState{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetPluginState() *ExecutableTaskNodeStatus_GetPluginState {
	c_call := _m.On("GetPluginState")
	return &ExecutableTaskNodeStatus_GetPluginState{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetPluginStateMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetPluginState {
	c_call := _m.On("GetPluginState", matchers...)
	return &ExecutableTaskNodeStatus_GetPluginState{Call: c_call}
}

// GetPluginState provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetPluginState() []byte {
	ret := _m.Called()

	var r0 []byte
	if rf, ok := ret.Get(0).(func() []byte); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	return r0
}

type ExecutableTaskNodeStatus_GetPluginStateVersion struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetPluginStateVersion) Return(_a0 uint32) *ExecutableTaskNodeStatus_GetPluginStateVersion {
	return &ExecutableTaskNodeStatus_GetPluginStateVersion{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetPluginStateVersion() *ExecutableTaskNodeStatus_GetPluginStateVersion {
	c_call := _m.On("GetPluginStateVersion")
	return &ExecutableTaskNodeStatus_GetPluginStateVersion{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetPluginStateVersionMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetPluginStateVersion {
	c_call := _m.On("GetPluginStateVersion", matchers...)
	return &ExecutableTaskNodeStatus_GetPluginStateVersion{Call: c_call}
}

// GetPluginStateVersion provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetPluginStateVersion() uint32 {
	ret := _m.Called()

	var r0 uint32
	if rf, ok := ret.Get(0).(func() uint32); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint32)
	}

	return r0
}

type ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath struct {
	*mock.Call
}

func (_m ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath) Return(_a0 storage.DataReference) *ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath {
	return &ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath{Call: _m.Call.Return(_a0)}
}

func (_m *ExecutableTaskNodeStatus) OnGetPreviousNodeExecutionCheckpointPath() *ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath {
	c_call := _m.On("GetPreviousNodeExecutionCheckpointPath")
	return &ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath{Call: c_call}
}

func (_m *ExecutableTaskNodeStatus) OnGetPreviousNodeExecutionCheckpointPathMatch(matchers ...interface{}) *ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath {
	c_call := _m.On("GetPreviousNodeExecutionCheckpointPath", matchers...)
	return &ExecutableTaskNodeStatus_GetPreviousNodeExecutionCheckpointPath{Call: c_call}
}

// GetPreviousNodeExecutionCheckpointPath provides a mock function with given fields:
func (_m *ExecutableTaskNodeStatus) GetPreviousNodeExecutionCheckpointPath() storage.DataReference {
	ret := _m.Called()

	var r0 storage.DataReference
	if rf, ok := ret.Get(0).(func() storage.DataReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(storage.DataReference)
	}

	return r0
}
