package resourcemanager

import (
	"context"

	rmConfig "github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
)

//go:generate mockery -name ResourceManager -case=underscore

type AllocationStatus string

const (
	// This is the enum returned when there's an error
	AllocationUndefined                    AllocationStatus = "ResourceGranted"

	// Go for it
	AllocationStatusGranted                AllocationStatus = "ResourceGranted"

	// This means that no resources are available globally.  This is the only rejection message we use right now.
	AllocationStatusExhausted              AllocationStatus = "ResourceExhausted"

	// We're not currently using this - but this would indicate that things globally are okay, but that your
	// own namespace is too busy
	AllocationStatusNamespaceQuotaExceeded AllocationStatus = "NamespaceQuotaExceeded"
)

type ResourceManagerType string

const (
	ResourceManagerTypeNoop  ResourceManagerType = "noop"
	ResourceManagerTypeRedis ResourceManagerType = "redis"
)

type ResourceNegotiator interface {
	RegisterResourceQuota(ctx context.Context, namespace string, quota int) Token, error
}

// Resource Manager manages a single resource type, and each allocation is of size one
type ResourceManager interface {
	//ResourceNegotiator
	AllocateResource(ctx context.Context, allocationToken string) (AllocationStatus, error)
	ReleaseResource(ctx context.Context, token Token, allocationToken string) error
}


// Gets or creates a resource manager to the given resource name. This function is thread-safe and calling it with the
// same resource name will return the same instance of resource manager every time.
func GetOrCreateResourceManagerFor(ctx context.Context, resourceName string) (ResourceManager, error) {
	return NoopResourceManager{}, nil
}

func GetResourceManagerByType(ctx context.Context, managerType ResourceManagerType, scope promutils.Scope) (
	ResourceManager, error) {

	switch managerType {
	case ResourceManagerTypeNoop:
		logger.Infof(ctx, "Using the NOOP resource manager")
		return NoopResourceManager{}, nil
	case ResourceManagerTypeRedis:
		logger.Infof(ctx, "Using Redis based resource manager")
		config := rmConfig.GetResourceManagerConfig()
		redisClient, err := NewRedisClient(ctx, config.RedisHostPath, config.RedisHostKey, config.RedisMaxRetries)
		if err != nil {
			logger.Errorf(ctx, "Unable to initialize a redis client for the resource manager: [%v]", err)
			return nil, err
		}
		return NewRedisResourceManager(ctx, redisClient, scope.NewSubScope("flytepropeller:resourcemanager:redis"))
	}
	logger.Infof(ctx, "Using the NOOP resource manager by default")
	return NoopResourceManager{}, nil
}