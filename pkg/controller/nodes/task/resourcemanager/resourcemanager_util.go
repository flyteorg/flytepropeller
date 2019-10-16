package resourcemanager

import (
	"context"
	rmConfig "github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
)

const (
	propellerPrometheusScope            = "flytepropeller"
	redisResourceManagerPrometheusScope = propellerPrometheusScope + ":" + "redisresourcemanager"
)


func GetResourceManagerByType(ctx context.Context, managerType Type, scope promutils.Scope) (
	Factory, error) {

	switch managerType {
	case TypeNoop:
		logger.Infof(ctx, "Using the NOOP resource manager")
		return &NoopResourceManager{}, nil
	case TypeRedis:
		logger.Infof(ctx, "Using Redis based resource manager")
		config := rmConfig.GetResourceManagerConfig()
		redisClient, err := NewRedisClient(ctx, config.RedisHostPath, config.RedisHostKey, config.RedisMaxRetries)
		if err != nil {
			logger.Errorf(ctx, "Unable to initialize a redis client for the resource manager: [%v]", err)
			return nil, err
		}
		return NewRedisResourceManager(ctx, redisClient, scope.NewSubScope(redisResourceManagerPrometheusScope))
	}
	logger.Infof(ctx, "Using the NOOP resource manager by default")
	return &NoopResourceManager{}, nil
}
