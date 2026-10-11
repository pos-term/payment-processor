package processor

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisDeduper marks transaction ids with SET NX and a TTL.
type RedisDeduper struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisDeduper(client *redis.Client, ttl time.Duration) *RedisDeduper {
	return &RedisDeduper{client: client, ttl: ttl}
}

func (d *RedisDeduper) FirstSeen(ctx context.Context, transactionID string) (bool, error) {
	return d.client.SetNX(ctx, "dedup:tx:"+transactionID, 1, d.ttl).Result()
}
