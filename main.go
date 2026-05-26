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

type Server struct {
	api.UnimplementedIdempotencyServiceServer
	db  *Storage
	rds *redis.Client
}

func (s *Server) CheckKey(ctx context.Context, req *api.CheckKeyRequest) (*api.CheckKeyResponse, error) {
	key := req.GetKey()

	cachedStatus, err := s.rds.Get(ctx, "cache:"+key).Result()

	if err == nil {
		log.Printf("Ура! Ключ %s найден в кэше Redis. Статус: %s", key, cachedStatus)
		return &api.CheckKeyResponse{
			Status: cachedStatus,
		}, nil

	}

	lockKey := "lock: " + key

	success, rdsError := s.rds.SetNX(ctx, lockKey, "in_progress", 10*time.Second).Result()

	if rdsError != nil {
		log.Fatalf("Ошибка Redis при попытке поставить замок: %v", rdsError)
	}

	if rdsError == nil && !success {
		log.Printf("Запрос с ключом %s заблокирован: дубликат уже обрабатывается", key)
		return &api.CheckKeyResponse{Status: "pending"}, nil
	}

	status, err := s.db.IsKeyExists(ctx, key)
	if err != nil {
		log.Printf("Ошибка при проверке ключа в Postgres: %v", err)
		return nil, err
	}

	switch status {
	case "success":
		log.Printf("Внимание: ключ %s НАЙДЕН в базе. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{
			Status: status,
		}, nil
	case "pending":
		log.Printf("Внимание: ключ %s ВСЕ ЕЩЕ в обработке. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{
			Status: status,
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

	if status != "success" && status != "failed" {
		return nil, fmt.Errorf("недопустимый статус платежа: %s", status)
	}

	err := s.db.SavePaymentResult(ctx, key, status, code, body)
	if err != nil {
		log.Printf("Не удалось сохранить результат в БД: %v", err)
		return nil, err
	}

	log.Printf("Результат платежа для ключа %s успешно сохранен в Postgres!", key)

	err = s.rds.Set(ctx, "cache:"+key, status, 24*time.Hour).Err()

	if err != nil {
		log.Printf("Не удалось сохранить статус в кэш Redis: %v", err)
	}

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

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	api.RegisterIdempotencyServiceServer(grpcServer, &Server{db: db, rds: rds})

	log.Println("gRPC server is running on port :50051...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
