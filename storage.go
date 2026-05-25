package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
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

func (s *Storage) IsKeyExists(ctx context.Context, key string) (string, error) {
	var status string

	query := "SELECT status FROM idempotency_key WHERE key = $1"

	err := s.pool.QueryRow(ctx, query, key).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "not_found", nil // возвращаем специальный статус "not_found"
		}

		return "", fmt.Errorf("ошибка при чтении статуса из БД: %w", err)
	}

	return status, nil
}

func (s *Storage) CreateKey(ctx context.Context, key string) error {
	query := `INSERT INTO idempotency_key (key, status, created_time, updated_time)
	VALUES ($1, 'pending', NOW(), NOW())`

	_, err := s.pool.Exec(ctx, query, key)

	if err != nil {
		return fmt.Errorf("не удалось сохранить новый ключ: %w", err)
	}

	return nil
}

func (s *Storage) UpdateKey(ctx context.Context, status string, key string) error {
	query := `UPDATE idempotency_key SET status = $1, updated_time = NOW() WHERE key = $2`

	result, err := s.pool.Exec(ctx, query, status, key)
	if err != nil {
		return fmt.Errorf("не удалось обновить статус ключа %s в БД: %w", key, err)
	}

	// Маленькая проверка для надежности: проверяем, что строка вообще обновилась
	if result.RowsAffected() == 0 {
		return fmt.Errorf("ключ %s не найден в базе данных для обновления", key)
	}

	return nil
}
