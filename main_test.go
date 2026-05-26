package main

import (
	"context"
	"log"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	// ВАЖНО: Замени "bifrost/api" на твой реальный путь к protobuf-пакету api
	"github.com/becxq/bifrost-pay/api"
)

func TestCheckKey_Concurrency(t *testing.T) {
	ctx := context.Background()

	// 1. Подключаемся к твоему локальному Redis
	rds := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rds.Close()

	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		log.Fatalf("Ошибка инициализации базы данных: %v", err)
	}

	// Очищаем ключи в Redis перед тестом, чтобы всё было честно
	testKey := "test_payment_12345"
	rds.Del(ctx, "lock:"+testKey)
	rds.Del(ctx, "cache:"+testKey)

	// 2. Создаем экземпляр нашего сервера
	// Передаем nil вместо базы данных, так как мы проверяем блокировку на самом входе в Redis
	srv := &Server{
		rds: rds,
		db:  db,
	}

	// Канал, куда 10 горутин скинут ответы сервера
	results := make(chan string, 10)
	var wg sync.WaitGroup

	// 3. Симулируем 10 ОДНОВРЕМЕННЫХ запросов (атака близнецов)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Вызываем твой метод CheckKey
			req := &api.CheckKeyRequest{Key: testKey}
			resp, err := srv.CheckKey(ctx, req)

			if err == nil && resp != nil {
				results <- resp.Status
			}
		}()
	}

	// Ждем, пока все 10 горутин отработают
	wg.Wait()
	close(results)

	// 4. Считаем, сколько запросов было отбито со статусом pending
	pendingCount := 0
	for status := range results {
		if status == "pending" {
			pendingCount++
		}
	}

	// Логика: 1-й запрос занял замок и пошел дальше. Остальные 9 должны получить "pending"
	expectedPending := 9
	if pendingCount != expectedPending {
		t.Errorf("Провал! Вышибала пропустил лишнее. Ожидали блок 9 запросов, а заблокировано: %d", pendingCount)
	} else {
		t.Logf("Успех! Из 10 одновременных запросов Redis четко заблокировал %d дубликатов.", pendingCount)
	}
}
