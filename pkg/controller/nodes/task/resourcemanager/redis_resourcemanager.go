package resourcemanager

import (
"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager/config"
	"k8s.io/apimachinery/pkg/util/wait"
	"time"
	rmConfig "github.com/lyft/flytepropeller/pkg/controller/nodes/task/resourcemanager/config"

"github.com/go-redis/redis"
"github.com/lyft/flytestdlib/logger"
"github.com/lyft/flytestdlib/promutils"
"github.com/prometheus/client_golang/prometheus"
)

// This is the key that will point to the Redis Set.
// https://redis.io/commands#set
const RedisSetKeyPrefix = "resourcemanager"

type RedisResourceManager struct {
	client      *redis.Client
	redisSetKeyPrefix string
	Metrics     RedisResourceManagerMetrics
}

type RedisResourceManagerMetrics struct {
	Scope                promutils.Scope
	RedisSizeCheckTime   promutils.StopWatch
	AllocatedTokensGauge *prometheus.GaugeVec
}

func NewRedisResourceManagerMetrics(scope promutils.Scope) RedisResourceManagerMetrics {
	return RedisResourceManagerMetrics{
		Scope: scope,
		RedisSizeCheckTime: scope.MustNewStopWatch("redis:size_check_time_ms",
			"The time it takes to measure the size of the Redis Set where all utilized resource are stored", time.Millisecond),

		AllocatedTokensGauge: scope.MustNewGauge("size",
			"The number of allocation tokens currently in the Redis set", "Resource"),
	}
}

func (r *RedisResourceManager) RegisterResourceRequest(ctx context.Context, namespace string, allocationToken string, quota int) error {
	if r.client == nil {
		err := errors.Errorf("Redis client does not exist.")
		return err
	}

	if quota <= 0 {
		err := errors.Errorf("Invalid request for resource quota (<= 0): [%v]", quota)
		return err
	}

	config := rmConfig.GetResourceManagerConfig()
	config.ResourceQuota = quota

	return nil
}

func (r RedisResourceManager) getNamespacedRedisSetKey(namespace string) string {
	return fmt.Sprintf("%s:%s", r.redisSetKeyPrefix, namespace)
}

func (r RedisResourceManager) AllocateResource(ctx context.Context, namespace string, allocationToken string) (
	AllocationStatus, error) {

	namespacedRedisSetKey := r.getNamespacedRedisSetKey(namespace)
	// Check to see if the allocation token is already in the set
	found, err := r.client.SIsMember(namespacedRedisSetKey, allocationToken).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set %v", err)
		return AllocationUndefined, err
	}
	if found {
		logger.Infof(ctx, "Already allocated [%s:%s]", namespace, allocationToken)
		return AllocationStatusGranted, nil
	}

	size, err := r.client.SCard(namespacedRedisSetKey).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set %v", err)
		return AllocationUndefined, err
	}

	if size > int64(config.GetResourceManagerConfig().ResourceQuota) {
		logger.Infof(ctx, "Too many allocations (total [%d]), rejecting [%s:%s]", size, namespace, allocationToken)
		return AllocationStatusExhausted, nil
	}

	countAdded, err := r.client.SAdd(namespacedRedisSetKey, allocationToken).Result()
	if err != nil {
		logger.Errorf(ctx, "Error adding token [%s:%s] %v", namespace, allocationToken, err)
		return AllocationUndefined, err
	}
	logger.Infof(ctx, "Added %d to the Redis Qubole set", countAdded)

	return AllocationStatusGranted, err
}

func (r RedisResourceManager) ReleaseResource(ctx context.Context, namespace string, allocationToken string) error {
	namespacedRedisSetKey := r.getNamespacedRedisSetKey(namespace)
	countRemoved, err := r.client.SRem(namespacedRedisSetKey, allocationToken).Result()
	if err != nil {
		logger.Errorf(ctx, "Error removing token [%s:%s] %v", namespace, allocationToken, err)
		return err
	}
	logger.Infof(ctx, "Removed %d token: %s", countRemoved, allocationToken)

	return nil
}

func (r *RedisResourceManager) pollRedis(ctx context.Context, namespace) {
	namespacedRedisSetKey := r.getNamespacedRedisSetKey(namespace)
	stopWatch := r.Metrics.RedisSizeCheckTime.Start()
	defer stopWatch.Stop()
	size, err := r.client.SCard(r.redisSetKey).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set in metrics poller %v", err)
		return
	}
	g, err := r.Metrics.AllocatedTokensGauge.GetMetricWithLabelValues(namespace)
	g.Set(float64(size))
}

func (r *RedisResourceManager) startMetricsGathering(ctx context.Context) {
	wait.Until(func(){r.pollRedis(ctx)}, 10 * time.Second, ctx.Done())
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.pollRedis(ctx)
			}
		}
	}()
}

func NewRedisResourceManager(ctx context.Context, client *redis.Client, scope promutils.Scope) (*RedisResourceManager, error) {

	/*
	// Creating a set in redis with the given namespace
	namespacedRedisSetKey := fmt.Sprintf("%s:%s", RedisSetKeyPrefix, namespace)

	// Checking if the namespace already exists
	found, err := client.Exists(namespacedRedisSetKey).Result()
	if err != nil {
		logger.Errorf(ctx, "Error getting size of Redis set %v", err)
		return nil, err
	}
	if found != 0 {
		logger.Infof(ctx, "Resource namespace already exists [%s]", namespace)
		return nil, nil
	}
	*/

	rm := &RedisResourceManager{
		client:      client,
		Metrics:     NewRedisResourceManagerMetrics(scope),
		redisSetKeyPrefix: RedisSetKeyPrefix,
	}
	rm.startMetricsGathering(ctx)

	return rm, nil
}


