package common

import (
	"github.com/lyft/flytepropeller/pkg/apis/flyteworkflow/v1alpha1"
	"github.com/lyft/flytepropeller/pkg/controller/executors"
	"github.com/lyft/flytepropeller/pkg/utils"
	"strconv"
)

func GenerateUniqueId(parentInfo executors.ImmutableParentInfo, nodeID string) (string, error) {
	var parentUniqueId v1alpha1.NodeID
	var parentRetryAttempt string

	if parentInfo != nil {
		parentUniqueId = parentInfo.GetUniqueID()
		parentRetryAttempt = strconv.Itoa(int(parentInfo.CurrentAttempt()))
	}

	return utils.FixedLengthUniqueIDForParts(20, parentUniqueId, parentRetryAttempt, nodeID)
}

func GetParentInfo(grandParentInfo executors.ImmutableParentInfo, nodeID string, parentAttempt uint32) (executors.ImmutableParentInfo, error){
	uniqueID, err := GenerateUniqueId(grandParentInfo, nodeID)
	if err != nil {
		return nil, err
	}
	return executors.NewParentInfo(uniqueID, parentAttempt), nil

}