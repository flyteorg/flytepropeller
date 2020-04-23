package utils

import (
	"testing"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/stretchr/testify/assert"
)

func TestToString(t *testing.T) {
	assert.Equal(t, "[SYSTEM|code]:message", ToString(&core.ExecutionError{Message: "message", Code: "code", Kind: core.ExecutionError_SYSTEM}))
}

func TestParseKind(t *testing.T) {
	assert.Equal(t, core.ExecutionError_SYSTEM, ParseKind("SYSTEM"))
	assert.Equal(t, core.ExecutionError_USER, ParseKind("USER"))
	assert.Equal(t, core.ExecutionError_USER, ParseKind("user"))
	assert.Equal(t, core.ExecutionError_USER, ParseKind("User"))
	assert.Equal(t, core.ExecutionError_UNKNOWN, ParseKind("xyz"))
	assert.Equal(t, core.ExecutionError_UNKNOWN, ParseKind("unknown"))
}

func TestParseExecutionError(t *testing.T) {
	t.Run("happy", func(t *testing.T) {
		m := "[SYSTEM|code]:message"
		assert.Equal(t, m, ToString(ParseExecutionError(m)))
	})

	t.Run("unhappy", func(t *testing.T) {
		m := "[code]:message"
		err := ParseExecutionError(m)
		assert.Equal(t, m, err.Message)
		assert.Equal(t, core.ExecutionError_UNKNOWN, err.Kind)
		assert.Equal(t, "Unknown", err.Code)
	})
}
