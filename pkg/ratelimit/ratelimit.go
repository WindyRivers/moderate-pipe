// Package ratelimit provides a Redis-backed token-bucket limiter. The Content
// Service uses it to cap the post-creation rate so a traffic burst cannot
// overwhelm the downstream moderation pipeline faster than it can be smoothed by
// Kafka. Redis (rather than an in-process limiter) is used so the limit is
// shared across all Content Service replicas.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// tokenBucket is evaluated atomically inside Redis so concurrent requests can
// never over-draw the bucket (a check-then-set in Go would race). It refills
// `rate` tokens per second up to `burst`, storing the token count and last
// refill timestamp in a small hash keyed per client.
var tokenBucket = redis.NewScript(`
local key      = KEYS[1]
local rate     = tonumber(ARGV[1])
local burst    = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local requested= tonumber(ARGV[4])

local data = redis.call("HMGET", key, "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

-- Refill based on elapsed time.
local delta = math.max(0, now - ts)
tokens = math.min(burst, tokens + delta * rate)

local allowed = 0
if tokens >= requested then
  tokens = tokens - requested
  allowed = 1
end

redis.call("HMSET", key, "tokens", tokens, "ts", now)
-- Expire idle buckets so we don't leak keys for one-off clients.
redis.call("PEXPIRE", key, math.ceil(burst / rate * 1000) + 1000)
return allowed
`)

// Limiter is a reusable token-bucket limiter.
type Limiter struct {
	rdb   *redis.Client
	rate  float64 // tokens per second
	burst float64 // bucket capacity
}

// New returns a limiter that permits `rate` requests/second with a burst
// allowance of `burst`.
func New(rdb *redis.Client, rate, burst float64) *Limiter {
	return &Limiter{rdb: rdb, rate: rate, burst: burst}
}

// Allow reports whether one request for the given key (e.g. "post:create" or a
// per-user key) may proceed right now. It fails open: if Redis is unreachable
// the request is allowed rather than blocking the whole endpoint on the
// limiter, since a limiter outage should not become a full outage.
func (l *Limiter) Allow(ctx context.Context, key string) bool {
	now := float64(time.Now().UnixMilli()) / 1000.0
	res, err := tokenBucket.Run(ctx, l.rdb, []string{"ratelimit:" + key},
		l.rate, l.burst, now, 1).Int()
	if err != nil {
		return true
	}
	return res == 1
}
