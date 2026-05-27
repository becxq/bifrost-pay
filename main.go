package main

import (
	"context"
	"log"
	"net"
	"time"

	"github.com/becxq/bifrost-pay/api"

	"google.golang.org/grpc"
)

type Server struct {
	api.UnimplementedIdempotencyServiceServer
	db  *Storage
	rdb *RedisDB
}

func main() {
	conn := "postgres://bifrost_user:bifrost_password@localhost:5433/bifrost_db?sslmode=disable"

	db, err := NewStorage(conn)
	if err != nil {
		log.Fatalf("Ошибка инициализации базы данных: %v", err)
	}

	ctx := context.Background()

	rdb := NewRedisDB("localhost:6379")

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	_, err = rdb.rdb.Ping(pingCtx).Result()
	if err != nil {
		log.Fatalf("Ошибка инициализации Redis: %v", err)
	}
	log.Println("Успешное подключение к Redis!")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	api.RegisterIdempotencyServiceServer(grpcServer, &Server{db: db, rdb: rdb})

	log.Println("gRPC server is running on port :50051...")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
