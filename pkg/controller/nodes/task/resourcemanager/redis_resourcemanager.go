package resourcemanager

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	pluginCore "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/core"
	rmConfig "github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
)

// This is the key that will point to the Redis Set.
// https://redis.io/commands#set
const RedisSetKeyPrefix = "resourcemanager"

type RedisResourceManagerBuilder struct {
	client                *redis.Client
	MetricsScope		  promutils.Scope
	redisSetKeyPrefix     string
	namespacedResourcesMap map[pluginCore.ResourceNamespace]*Resource
}

func (r *RedisResourceManagerBuilder) ResourceRegistrar(namespacePrefix pluginCore.ResourceNamespace) pluginCore.ResourceRegistrar {
	return ResourceRegistrarProxy{
		ResourceRegistrar:  r,
		NamespacePrefix:    namespacePrefix,
	}
}

func (r *RedisResourceManagerBuilder) RegisterResourceQuota(ctx context.Context, namespace pluginCore.ResourceNamespace, quota int) error {

	if r.client == nil {
		err := errors.Errorf("Redis client does not exist.")
		return err
	}
	config := rmConfig.GetResourceManagerConfig()
	if quota <= 0 || quota > config.ResourceMaxQuota {
		err := errors.Errorf("Invalid request for resource quota (<= 0 || > %v): [%v]", config.ResourceMaxQuota, quota)
		return err
	}

	// Checking if the namespace already exists
	// We use linear search here because this function is only called a few times
	if _, ok := r.namespacedResourcesMap[namespace]; ok {
		return errors.Errorf("Resource namespace already exists [%v]", namespace)
	}

	// TODO: add this back. Do this when building the manager

	prefixedNamespace := r.getNamespacedRedisSetKey(namespace)


	metrics := NewRedisResourceManagerMetrics(r.MetricsScope.NewSubScope(prefixedNamespace))

	newResource := &Resource{
		quota:   quota,
		metrics: metrics,
	}

	// Add this registration to the list
	r.namespacedResourcesMap[namespace] = newResource
	return nil
}

func (r *RedisResourceManagerBuilder) BuildResourceManager(ctx context.Context) (pluginCore.ResourceManager, error) {
	if r.client == nil || r.redisSetKeyPrefix == "" || r.MetricsScope == nil || r.namespacedResourcesMap == nil {
		return nil, errors.Errorf("Failed to build a redis resource manager. Missing key property(s)")
	}
	rm := &RedisResourceManager{
		client:                 r.client,
		redisSetKeyPrefix:      r.redisSetKeyPrefix,
		MetricsScope:           r.MetricsScope,
		namespacedResourcesMap: r.namespacedResourcesMap,
	}
	rm.startMetricsGathering(ctx)
	return rm, nil
}

func (r *RedisResourceManagerBuilder) getNamespacedRedisSetKey(namespace pluginCore.ResourceNamespace) string {
	return fmt.Sprintf("%s:%s", r.redisSetKeyPrefix, namespace)
}

func NewRedisResourceManagerBuilder(ctx context.Context, client *redis.Client, scope promutils.Scope) (*RedisResourceManagerBuilder, error) {
	rn := &RedisResourceManagerBuilder{
		client:                 client,
		MetricsScope:           scope,
		redisSetKeyPrefix:      RedisSetKeyPrefix,
		namespacedResourcesMap: map[pluginCore.ResourceNamespace]*Resource{},
	}
	return rn, nil
}



type RedisResourceManager struct {
	client                 *redis.Client
	redisSetKeyPrefix      string
	MetricsScope           promutils.Scope
	namespacedResourcesMap map[pluginCore.ResourceNamespace]*Resource
}

func (r *RedisResourceManager) GetTaskResourceManager(namespacePrefix pluginCore.ResourceNamespace) pluginCore.ResourceManager {
	return Proxy{
		ResourceManager:    r,
		NamespacePrefix:    namespacePrefix,
	}
}

type RedisResourceManagerMetrics struct {
	Scope                promutils.Scope
	RedisSizeCheckTime   promutils.StopWatch
	AllocatedTokensGauge prometheus.Gauge
}



func (rrmm RedisResourceManagerMetrics) GetScope() promutils.Scope {
	return rrmm.Scope
}

func (r *RedisResourceManager) getResource(namespace pluginCore.ResourceNamespace) *Resource {
	return r.namespacedResourcesMap[namespace]
}

func (r *RedisResourceManager) pollRedis(ctx context.Context, namespace pluginCore.ResourceNamespace) {
	namespacedRedisSetKey := r.getNamespacedRedisSetKey(namespace)
	stopWatch := r.getResource(namespace).metrics.(*RedisResourceManagerMetrics).RedisSizeCheckTime.Start()
	defer stopWatch.Stop()
	size, err := r.client.SCard(namespacedRedisSetKey).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set in metrics poller %v", err)
		return
	}
	metrics := r.getResource(namespace).metrics.(*RedisResourceManagerMetrics)
	metrics.AllocatedTokensGauge.Set(float64(size))
	// g, err := metrics.AllocatedTokensGauge
	// g.Set(float64(size))
}

func (r *RedisResourceManager) startMetricsGathering(ctx context.Context) {
	wait.Until(func() {
		for namespace := range r.namespacedResourcesMap {
			r.pollRedis(ctx, namespace)
		}
	}, 10*time.Second, ctx.Done())
	/*
		ticker := time.NewTicker(10 * time.Second)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					for _, rrToken := range r.resourceRegistryTokens {
						r.pollRedis(ctx, rrToken)
					}
				}
			}
		}()
	*/
}

func NewRedisResourceManagerMetrics(scope promutils.Scope) *RedisResourceManagerMetrics {
	return &RedisResourceManagerMetrics{
		Scope: scope,
		RedisSizeCheckTime: scope.MustNewStopWatch("redis:size_check_time_ms",
			"The time it takes to measure the size of the Redis Set where all utilized resource are stored", time.Millisecond),

		AllocatedTokensGauge: scope.MustNewGauge("size",
			"The number of allocation resourceRegistryTokens currently in the Redis set"),
	}
}



func (r *RedisResourceManager) getNamespacedRedisSetKey(namespace pluginCore.ResourceNamespace) string {
	return fmt.Sprintf("%s:%s", r.redisSetKeyPrefix, namespace)
}

func (r *RedisResourceManager) AllocateResource(ctx context.Context, namespace pluginCore.ResourceNamespace, allocationToken string) (
	pluginCore.AllocationStatus, error) {

	namespacedRedisSetKey := r.getNamespacedRedisSetKey(namespace)
	namespacedResource := r.getResource(namespace)
	// Check to see if the allocation token is already in the set
	found, err := r.client.SIsMember(namespacedRedisSetKey, allocationToken).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set %v", err)
		return pluginCore.AllocationUndefined, err
	}
	if found {
		logger.Infof(ctx, "Already allocated [%s:%s]", namespace, allocationToken)
		return pluginCore.AllocationStatusGranted, nil
	}

	size, err := r.client.SCard(namespacedRedisSetKey).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set %v", err)
		return pluginCore.AllocationUndefined, err
	}

	if size > int64(namespacedResource.quota) {
		logger.Infof(ctx, "Too many allocations (total [%d]), rejecting [%s:%s]", size, namespace, allocationToken)
		return pluginCore.AllocationStatusExhausted, nil
	}

	countAdded, err := r.client.SAdd(namespacedRedisSetKey, allocationToken).Result()
	if err != nil {
		logger.Errorf(ctx, "Error adding token [%s:%s] %v", namespace, allocationToken, err)
		return pluginCore.AllocationUndefined, err
	}
	logger.Infof(ctx, "Added %d to the Redis Qubole set", countAdded)

	return pluginCore.AllocationStatusGranted, err
}

func (r *RedisResourceManager) ReleaseResource(ctx context.Context, namespace pluginCore.ResourceNamespace, allocationToken string) error {
	namespacedRedisSetKey := r.getNamespacedRedisSetKey(namespace)
	countRemoved, err := r.client.SRem(namespacedRedisSetKey, allocationToken).Result()
	if err != nil {
		logger.Errorf(ctx, "Error removing token [%v:%s] %v", namespace, allocationToken, err)
		return err
	}
	logger.Infof(ctx, "Removed %d token: %s", countRemoved, allocationToken)

	return nil
}


