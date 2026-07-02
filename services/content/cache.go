package content

import (
	"context"
	"time"

	"github.com/WindyRivers/moderate-pipe/internal/model"
	"github.com/redis/go-redis/v9"
)

// StatusCache caches a post's moderation status in Redis so the hot "is my post
// approved yet?" polling path does not hit MySQL on every call. The review
// result consumer writes through to this cache the moment a decision lands, so
// the cached value is fresh rather than merely eventually-correct.
type StatusCache struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewStatusCache(rdb *redis.Client) *StatusCache {
	return &StatusCache{rdb: rdb, ttl: 10 * time.Minute}
}

func (c *StatusCache) key(postID uint) string {
	return "post:status:" + itoa(postID)
}

// Get returns the cached status and whether it was present.
func (c *StatusCache) Get(ctx context.Context, postID uint) (model.ReviewStatus, bool) {
	v, err := c.rdb.Get(ctx, c.key(postID)).Result()
	if err != nil {
		return "", false
	}
	return model.ReviewStatus(v), true
}

// Set writes the status through to the cache with a TTL.
func (c *StatusCache) Set(ctx context.Context, postID uint, status model.ReviewStatus) {
	c.rdb.Set(ctx, c.key(postID), string(status), c.ttl)
}

func itoa(u uint) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}
