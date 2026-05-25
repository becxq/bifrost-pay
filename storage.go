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
