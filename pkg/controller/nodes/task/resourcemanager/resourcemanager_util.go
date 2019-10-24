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

func GetResourceManagerBuilderByType(ctx context.Context, managerType rmConfig.Type, scope promutils.Scope) (
	Builder, error) {

	switch managerType {
	case rmConfig.TypeNoop:
		logger.Infof(ctx, "Using the NOOP resource manager")
		return &NoopResourceManagerBuilder{}, nil
	case rmConfig.TypeRedis:
		logger.Infof(ctx, "Using Redis based resource manager")
		config := rmConfig.GetResourceManagerConfig()
		redisClient, err := NewRedisClient(ctx, config.RedisHostPath, config.RedisHostKey, config.RedisMaxRetries)
		if err != nil {
			logger.Errorf(ctx, "Unable to initialize a redis client for the resource manager: [%v]", err)
			return nil, err
		}
		return NewRedisResourceManagerBuilder(ctx, redisClient, scope.NewSubScope(redisResourceManagerPrometheusScope))
	}
	logger.Infof(ctx, "Using the NOOP resource manager by default")
	return &NoopResourceManagerBuilder{}, nil
}
