package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisDB struct {
	rdb *redis.Client
}

func NewRedisDB(addr string) *RedisDB {
	return &RedisDB{rdb: redis.NewClient(&redis.Options{Addr: addr, Password: "", DB: 0})}
}

func (r *RedisDB) SetNX(ctx context.Context, key string) (bool, error) {
	lockKey := "lock: " + key
	success, err := r.rdb.SetNX(ctx, lockKey, "in_progress", 10*time.Second).Result()

	if err != nil {
		return success, nil
	}

	return success, nil
}

func (r *RedisDB) Set(ctx context.Context, key string, status string) error {
	return r.rdb.Set(ctx, "cache:"+key, status, 24*time.Hour).Err()
}

func (r *RedisDB) Get(ctx context.Context, key string) (string, error) {
	return r.rdb.Get(ctx, "cache:"+key).Result()
}
