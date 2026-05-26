package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/becxq/bifrost-pay/api"
	"github.com/redis/go-redis/v9"

	"google.golang.org/grpc"
)

// Server — наша структура, которая будет обрабатывать gRPC запросы
type Server struct {
	api.UnimplementedIdempotencyServiceServer
	db  *Storage
	rds *redis.Client
}

// CheckKey — реализация нашего gRPC метода
func (s *Server) CheckKey(ctx context.Context, req *api.CheckKeyRequest) (*api.CheckKeyResponse, error) {
	key := req.GetKey()

	status, err := s.db.IsKeyExists(ctx, key)
	if err != nil {
		log.Printf("Ошибка при проверке ключа в Postgres: %v", err)
		// Если база данных упала, мы возвращаем ошибку, чтобы gRPC сообщил клиенту о сбое
		return nil, err
	}

	switch status {
	case "success":
		log.Printf("Внимание: ключ %s НАЙДЕН в базе. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{
			Status: status, // Говорим клиенту: всё ок, этот запрос мы уже обработали
		}, nil
	case "pending":
		log.Printf("Внимание: ключ %s ВСЕ ЕЩЕ в обработке. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{
			Status: status, // Говорим клиенту: что запрос обрабатывается
		}, nil
	case "not_found":
		err = s.db.CreateKey(ctx, key)
		if err != nil {
			log.Printf("Не удалось забронировать ключ в БД: %v", err)
			return nil, err
		}

		return &api.CheckKeyResponse{
			Status: "not_found",
		}, nil
	case "failed":
		return &api.CheckKeyResponse{
			Status: "failed",
		}, nil
	}

	return &api.CheckKeyResponse{Status: "unknown"}, nil
}

func (s *Server) ConfirmKey(ctx context.Context, req *api.ConfirmKeyRequest) (*api.ConfirmKeyResponse, error) {
	key := req.GetKey()
	status := req.GetStatus()
	code := req.GetCode()
	body := req.GetBody()

	log.Printf("Получен запрос ConfirmKey для ключа: %s. Новый статус: %s", key, status)

	// 2. Валидируем входящий статус на стороне Go (для безопасности)
	if status != "success" && status != "failed" {
		return nil, fmt.Errorf("недопустимый статус платежа: %s", status)
	}

	// 3. Стучимся в базу и сохраняем финальный результат
	err := s.db.SavePaymentResult(ctx, key, status, code, body)
	if err != nil {
		log.Printf("Не удалось сохранить результат в БД: %v", err)
		return nil, err // Если база упала, возвращаем системную ошибку
	}

	log.Printf("Результат платежа для ключа %s успешно сохранен в Postgres!", key)

	// 4. Возвращаем клиенту флаг успеха
	return &api.ConfirmKeyResponse{
		Success: true,
	}, nil
}

func main() {
	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		log.Fatalf("Ошибка инициализации базы данных: %v", err)
	}

	ctx := context.Background()

	rds := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err = rds.Ping(pingCtx).Result()
	if err != nil {
		log.Fatalf("Ошибка инициализации Redis: %v", err)
	}
	log.Println("Успешное подключение к Redis!")

	// 1. Открываем TCP-порт для прослушивания
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 2. Создаем новый gRPC сервер
	grpcServer := grpc.NewServer()

	api.RegisterIdempotencyServiceServer(grpcServer, &Server{db: db, rds: rds})

	log.Println("gRPC server is running on port :50051...")

	// 4. Запускаем сервер
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
