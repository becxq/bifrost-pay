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
