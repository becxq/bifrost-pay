package main

import (
	"context"
	"log"
	"net"

	"github.com/becxq/bifrost-pay/api"

	"google.golang.org/grpc"
)

// Server — наша структура, которая будет обрабатывать gRPC запросы
type Server struct {
	api.UnimplementedIdempotencyServiceServer
	db *Storage
}

// CheckKey — реализация нашего gRPC метода
func (s *Server) CheckKey(ctx context.Context, req *api.CheckKeyRequest) (*api.CheckKeyResponse, error) {
	key := req.GetKey()

	exists, err := s.db.IsKeyExists(ctx, key)
	if err != nil {
		log.Printf("Ошибка при проверке ключа в Postgres: %v", err)
		// Если база данных упала, мы возвращаем ошибку, чтобы gRPC сообщил клиенту о сбое
		return nil, err
	}

	if exists {
		log.Printf("Внимание: ключ %s НАЙДЕН в базе. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{
			Status: "completed", // Говорим клиенту: всё ок, этот запрос мы уже обработали
		}, nil
	}

	log.Printf("Ключ %s НЕ НАЙДЕН в базе. Запрос уникальный, можно проводить оплату.", key)
	return &api.CheckKeyResponse{
		Status: "not_found", // Сигнализируем, что такого ключа мы еще не видели
	}, nil
}

func main() {
	conn := "postgres://bifrost_user:bifrost_password@localhost:5432/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		log.Fatalf("Ошибка инициализации базы данных: %v", err)
	}

	// 1. Открываем TCP-порт для прослушивания
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// 2. Создаем новый gRPC сервер
	grpcServer := grpc.NewServer()

	api.RegisterIdempotencyServiceServer(grpcServer, &Server{db: db})

	log.Println("gRPC server is running on port :50051...")

	// 4. Запускаем сервер
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
