package store

import (
	"context"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/config"
	"github.com/redis/go-redis/v9"
)

// NewRedis opens the Redis client shared by the Content Service (moderation
// status cache + rate limiter) and can be reused elsewhere. A short ping
// confirms connectivity at boot so misconfiguration fails fast.
func NewRedis(cfg *config.Config) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return rdb, nil
}
