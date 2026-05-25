package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	pool *pgxpool.Pool
}

func NewStorage(conn string) (*Storage, error) {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать пул соединений: %w", err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("база данных недоступна: %w", err)
	}

	log.Println("Успешное подключение к PostgreSQL пул запущен")
	return &Storage{pool: pool}, nil
}

func (s *Storage) IsKeyExists(ctx context.Context, key string) (bool, error) {
	var exists bool

	query := "SELECT EXISTS(SELECT 1 FROM idempotency_key WHERE key = $1)"

	err := s.pool.QueryRow(ctx, query, key).Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}
