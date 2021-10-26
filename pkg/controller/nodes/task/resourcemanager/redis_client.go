package resourcemanager

import (
	"context"

	"github.com/flyteorg/flytepropeller/pkg/controller/nodes/task/resourcemanager/config"

	"github.com/flyteorg/flytestdlib/logger"
	"github.com/go-redis/redis"
)

//go:generate mockery -name RedisClient -case=underscore

type RedisClient interface {
	// A pass-through method. Getting the cardinality of the Redis set
	SCard(string) (int64, error)
	// A pass-through method. Checking if an entity is a member of the set specified by the key
	SIsMember(string, interface{}) (bool, error)
	// A pass-through method. Adding an entity to the set specified by the key
	SAdd(string, interface{}) (int64, error)
	// A pass-through method. Removing an entity from the set specified by the key
	SRem(string, interface{}) (int64, error)
	// A pass-through method. Getting the complete list of MEMBERS of the set
	SMembers(string) ([]string, error)
	// A pass-through method. Pinging the Redis client
	Ping() (string, error)
}

type Redis struct {
	c redis.UniversalClient
}

func (r *Redis) SCard(key string) (int64, error) {
	return r.c.SCard(key).Result()
}

func (r *Redis) SIsMember(key string, member interface{}) (bool, error) {
	return r.c.SIsMember(key, member).Result()
}

func (r *Redis) SAdd(key string, member interface{}) (int64, error) {
	return r.c.SAdd(key, member).Result()
}

func (r *Redis) SRem(key string, member interface{}) (int64, error) {
	return r.c.SRem(key, member).Result()
}

func (r *Redis) SMembers(key string) ([]string, error) {
	return r.c.SMembers(key).Result()
}

func (r *Redis) Ping() (string, error) {
	return r.c.Ping().Result()
}

func NewRedisClient(ctx context.Context, cfg config.RedisConfig) (RedisClient, error) {
	// Backward compatibility
	if len(cfg.HostPaths) == 0 && len(cfg.HostPath) > 0 {
		cfg.HostPaths = []string{cfg.HostPath}
	}

	client := &Redis{
		c: redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:      cfg.HostPaths,
			MasterName: cfg.PrimaryName,
			Password:   cfg.HostKey,
			DB:         0, // use default DB
			MaxRetries: cfg.MaxRetries,
			TLSConfig:  config.ParseTLSConfig(cfg.TLSConfig),
		}),
	}

	_, err := client.Ping()
	if err != nil {
		logger.Errorf(ctx, "Error creating Redis client at [%+v]. Error: %v", cfg.HostPaths, err)
		return nil, err
	}

	logger.Infof(ctx, "Created Redis client with host [%+v]...", cfg.HostPaths)
	return client, nil
}
