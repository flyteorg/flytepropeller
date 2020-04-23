package utils

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
)

func ToString(err *core.ExecutionError) string {
	return fmt.Sprintf("[%s|%s]:%s", err.Kind, err.Code, err.Message)
}

func ParseKind(s string) core.ExecutionError_ErrorKind {
	switch strings.ToUpper(s) {
	case core.ExecutionError_SYSTEM.String():
		return core.ExecutionError_SYSTEM
	case core.ExecutionError_USER.String():
		return core.ExecutionError_USER
	}
	return core.ExecutionError_UNKNOWN
}

var re = regexp.MustCompile(`^\[(\w+)\|(\w+)]:(\w+)$`)

func ParseExecutionError(s string) *core.ExecutionError {
	groups := re.FindAllStringSubmatch(s, 3)
	if len(groups) == 1 {
		if len(groups[0]) == 4 {
			// Ignore the first match as that is always the entire string
			return &core.ExecutionError{
				Code:    groups[0][2],
				Message: groups[0][3],
				Kind:    ParseKind(groups[0][1]),
			}
		}
	}
	return &core.ExecutionError{
		Code:    "Unknown",
		Message: s,
	}
}
