package main

import (
	"context"
	"fmt"
	"log"

	"github.com/becxq/bifrost-pay/api"
)

func (s *Server) CheckKey(ctx context.Context, req *api.CheckKeyRequest) (*api.CheckKeyResponse, error) {
	key := req.GetKey()

	cachedStatus, err := s.rdb.Get(ctx, key)

	if err == nil {
		log.Printf("Ура! Ключ %s найден в кэше Redis. Статус: %s", key, cachedStatus)
		return &api.CheckKeyResponse{
			Status: cachedStatus,
		}, nil

	}

	success, rdbError := s.rdb.SetNX(ctx, key)
	if rdbError == nil && !success {
		log.Printf("Запрос с ключом %s заблокирован: дубликат уже обрабатывается", key)
		return &api.CheckKeyResponse{Status: "pending"}, nil
	} else if success {
		log.Printf("Запрос с ключом %s допущен!", key)
		return &api.CheckKeyResponse{Status: "success"}, nil
	}

	status, err := s.db.IsKeyExists(ctx, key)
	if err != nil {
		log.Printf("Ошибка при проверке ключа в Postgres: %v", err)
		return nil, err
	}

	switch status {
	case "success":
		log.Printf("Внимание: ключ %s НАЙДЕН в базе. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{Status: status}, nil
	case "pending":
		log.Printf("Внимание: ключ %s ВСЕ ЕЩЕ в обработке. Запрос отклонен как дубликат.", key)
		return &api.CheckKeyResponse{Status: status}, nil
	case "not_found":
		err = s.db.CreateKey(ctx, key)
		if err != nil {
			log.Printf("Не удалось забронировать ключ в БД: %v", err)
			return nil, err
		}

		return &api.CheckKeyResponse{Status: "not_found"}, nil
	case "failed":
		return &api.CheckKeyResponse{Status: "failed"}, nil
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

	err = s.rdb.Set(ctx, key, status)

	if err != nil {
		log.Printf("Не удалось сохранить статус в кэш Redis: %v", err)
	}

	return &api.ConfirmKeyResponse{
		Success: true,
	}, nil
}
