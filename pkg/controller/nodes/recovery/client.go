package recovery

import (
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/admin"
	"github.com/flyteorg/flyteidl/gen/pb-go/flyteidl/core"
)

type RecoveryClient interface {
	RecoverNodeExecution(id *core.NodeExecutionIdentifier) (admin.NodeExecution, error)
	RecoverNodeExecutionData(id *core.NodeExecutionIdentifier) (admin.NodeExecutionGetDataResponse, error)
}

type recoveryClient struct {

}

func (c *recoveryClient) RecoverNodeExecution(id *core.NodeExecutionIdentifier) (admin.NodeExecution, error) {

}


func NewRecoveryClient() RecoveryClient {
	return &recoveryClient{}
}
