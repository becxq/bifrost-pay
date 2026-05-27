package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/becxq/bifrost-pay/api"
	"github.com/google/uuid"
)

// setupTestServer инициализирует реальное подключение к тестовым БД.
// В идеале параметры должны браться из переменных окружения.
func setupTestServer(t testing.TB) *Server {
	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"
	db, err := NewStorage(conn)
	if err != nil {
		t.Fatalf("Ошибка БД: %v", err)
	}

	rdb := NewRedisDB("localhost:6379")

	return &Server{
		db:  db,
		rdb: rdb,
	}
}

// -----------------------------------------------------------------------------
// 1. ТЕСТЫ НА БЕЗОПАСНОСТЬ (CONCURRENCY / THUNDERING HERD)
// -----------------------------------------------------------------------------

// TestCheckKey_Concurrency проверяет, что при одновременном поступлении
// 100 запросов с одним и тем же ключом, только ОДИН получит статус "not_found"
// (и право на выполнение), а остальные 99 получат "pending".
func TestCheckKey_Concurrency(t *testing.T) {
	srv := setupTestServer(t)
	ctx := context.Background()
	testKey := "test-idemp-key-" + uuid.New().String()

	goroutinesCount := 100
	var wg sync.WaitGroup
	startCh := make(chan struct{}) // Канал для синхронного старта всех горутин

	var (
		notFoundCount atomic.Int32
		pendingCount  atomic.Int32
		errorCount    atomic.Int32
	)

	wg.Add(goroutinesCount)
	for i := 0; i < goroutinesCount; i++ {
		go func() {
			defer wg.Done()
			<-startCh // Ждем сигнала к началу, чтобы создать максимальную конкуренцию

			resp, err := srv.CheckKey(ctx, &api.CheckKeyRequest{Key: testKey})
			if err != nil {
				errorCount.Add(1)
				return
			}

			if resp.Status == "not_found" {
				notFoundCount.Add(1)
			} else if resp.Status == "pending" {
				pendingCount.Add(1)
			}
		}()
	}

	// Даем горутинам время на инициализацию и подаем сигнал к одновременному старту
	time.Sleep(100 * time.Millisecond)
	close(startCh)

	// Ждем завершения всех запросов
	wg.Wait()

	// Проверяем инварианты безопасности идемпотентности
	if errorCount.Load() > 0 {
		t.Fatalf("Получены ошибки при конкурентных запросах: %d", errorCount.Load())
	}

	if notFoundCount.Load() != 1 {
		t.Errorf("Ожидался ровно 1 допуск (not_found), но получено: %d", notFoundCount.Load())
	}

	if pendingCount.Load() != int32(goroutinesCount-1) {
		t.Errorf("Ожидалось %d блокировок (pending), но получено: %d", goroutinesCount-1, pendingCount.Load())
	}
}

// -----------------------------------------------------------------------------
// 2. БЕНЧМАРКИ НА СКОРОСТЬ (PERFORMANCE)
// -----------------------------------------------------------------------------

// BenchmarkCheckKey_Parallel тестирует пропускную способность метода CheckKey.
func BenchmarkCheckKey_Parallel(b *testing.B) {
	srv := setupTestServer(b)
	ctx := context.Background()

	b.ResetTimer() // Сбрасываем таймер после тяжелой инициализации БД

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Генерируем уникальный ключ для каждой итерации, чтобы проверить
			// скорость работы с новыми транзакциями.
			// Для большей скорости в бенчмарках можно использовать быстрые генераторы случайных строк.
			key := fmt.Sprintf("bench-key-%d", time.Now().UnixNano())

			_, err := srv.CheckKey(ctx, &api.CheckKeyRequest{Key: key})
			if err != nil {
				b.Fatalf("Ошибка в бенчмарке: %v", err)
			}
		}
	})
}

// BenchmarkCheckKey_Cached тестирует скорость чтения уже закэшированных ключей из Redis.
func BenchmarkCheckKey_Cached(b *testing.B) {
	srv := setupTestServer(b)
	ctx := context.Background()
	key := "bench-cached-key"

	// Предварительно сохраняем в кэш
	_ = srv.rdb.Set(ctx, key, "success")

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := srv.CheckKey(ctx, &api.CheckKeyRequest{Key: key})
			if err != nil || resp.Status != "success" {
				b.Fatalf("Ожидался success, получено: %v (ошибка: %v)", resp, err)
			}
		}
	})
}
