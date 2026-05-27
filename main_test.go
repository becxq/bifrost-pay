package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	startGate := make(chan struct{})

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGate

			req := &api.CheckKeyRequest{Key: testKey}
			resp, err := srv.CheckKey(ctx, req)

			if err == nil && resp != nil {
				results <- resp.Status
			}
		}()
	}

	close(startGate)

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
		t.Errorf("Провал! Ожидали блок 9 запросов, а заблокировано: %d", pendingCount)
	} else {
		t.Logf("Успех! Из 10 одновременных запросов Redis четко заблокировал %d дубликатов.", pendingCount)
	}
}

func TestCheckKey_RedisFailureFallback(t *testing.T) {
	ctx := context.Background()

	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		t.Fatalf("Ошибка БД: %v", err)
	}

	rds := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	srv := &Server{rds: rds, db: db}
	testKey := "fallback_key_" + time.Now().Format("150405")

	rds.Close()

	req := &api.CheckKeyRequest{Key: testKey}
	resp, err := srv.CheckKey(ctx, req)

	if err != nil {
		t.Fatalf("Провал! Сервис упал вместе с Redis, а должен был выжить: %v", err)
	}

	if resp == nil || resp.Status == "" {
		t.Error("Провал! Сервис вернул пустой ответ при упавшем Redis")
	}

	t.Logf("Успех! Redis мертв, но сервис устоял и вернул статус из Postgres: %s", resp.Status)
}

func TestCheckKey_ChaosLoad(t *testing.T) {
	ctx := context.Background()
	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		t.Fatalf("Ошибка БД: %v", err)
	}

	rds := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rds.Close()

	srv := &Server{rds: rds, db: db}

	keys := []string{"chaos_1", "chaos_2", "chaos_3", "chaos_4", "chaos_5"}
	for _, k := range keys {
		rds.Del(ctx, "lock:"+k)
		rds.Del(ctx, "cache:"+k)
	}

	numRequests := 100
	var wg sync.WaitGroup

	var errorCount int32

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Каждая горутина случайно выбирает один из 5 ключей
			// Это создаст жесткую конкуренцию за замки
			randomKey := keys[workerID%len(keys)]

			req := &api.CheckKeyRequest{Key: randomKey}
			_, err := srv.CheckKey(ctx, req)

			if err != nil {
				atomic.AddInt32(&errorCount, 1)
			}
		}(i)
	}

	wg.Wait()

	if errorCount > 0 {
		t.Errorf("Провал! Под хаотичной нагрузкой сервис выдал %d ошибок", errorCount)
	} else {
		t.Logf("Успех! Сдержали удар из %d перемешанных запросов. Ноль ошибок.", numRequests)
	}
}
