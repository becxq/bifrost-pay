package main

import (
	"context"
	"log"
	"sync"
	"testing"

	"github.com/becxq/bifrost-pay/api"
	"github.com/redis/go-redis/v9"
)

func TestCheckKey_Concurrency(t *testing.T) {
	ctx := context.Background()

	rds := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rds.Close()

	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		log.Fatalf("Ошибка инициализации базы данных: %v", err)
	}

	testKey := "test_payment_12345"
	rds.Del(ctx, "lock:"+testKey)
	rds.Del(ctx, "cache:"+testKey)

	srv := &Server{
		rds: rds,
		db:  db,
	}

	results := make(chan string, 10)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := &api.CheckKeyRequest{Key: testKey}
			resp, err := srv.CheckKey(ctx, req)

			if err == nil && resp != nil {
				results <- resp.Status
			}
		}()
	}

	wg.Wait()
	close(results)

	pendingCount := 0
	for status := range results {
		if status == "pending" {
			pendingCount++
		}
	}

	expectedPending := 9
	if pendingCount != expectedPending {
		t.Errorf("Провал! Вышибала пропустил лишнее. Ожидали блок 9 запросов, а заблокировано: %d", pendingCount)
	} else {
		t.Logf("Успех! Из 10 одновременных запросов Redis четко заблокировал %d дубликатов.", pendingCount)
	}
}
