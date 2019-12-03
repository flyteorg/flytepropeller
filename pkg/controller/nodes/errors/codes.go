package errors

import "github.com/lyft/flytestdlib/errors"

type ErrorCode = errors.ErrorCode

const (
	UnknownError                       ErrorCode = "UnknownError"
	InitializationError                ErrorCode = "InitializationError"
	NotYetImplementedError             ErrorCode = "NotYetImplementedError"
	DownstreamNodeNotFoundError        ErrorCode = "DownstreamNodeNotFound"
	UserProvidedError                  ErrorCode = "UserProvidedError"
	IllegalStateError                  ErrorCode = "IllegalStateError"
	BadSpecificationError              ErrorCode = "BadSpecificationError"
	UnsupportedTaskTypeError           ErrorCode = "UnsupportedTaskType"
	BindingResolutionError             ErrorCode = "BindingResolutionError"
	CausedByError                      ErrorCode = "CausedByError"
	RuntimeExecutionError              ErrorCode = "RuntimeExecutionError"
	SubWorkflowExecutionFailed         ErrorCode = "SubWorkflowExecutionFailed"
	RemoteChildWorkflowExecutionFailed ErrorCode = "RemoteChildWorkflowExecutionFailed"
	NoBranchTakenError                 ErrorCode = "NoBranchTakenError"
	OutputsNotFoundError               ErrorCode = "OutputsNotFoundError"
	StorageError                       ErrorCode = "StorageError"
	EventRecordingFailed               ErrorCode = "EventRecordingFailed"
	CatalogCallFailed                  ErrorCode = "CatalogCallFailed"
)
