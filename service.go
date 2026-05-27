package main

import (
	"context"
	"fmt"
	"log"

	"github.com/becxq/bifrost-pay/api"
)

func (s *Server) CheckKey(ctx context.Context, req *api.CheckKeyRequest) (*api.CheckKeyResponse, error) {
	key := req.GetKey()

	cachedStatus, err := s.rdb.Get(ctx, key) // Запрашиваем ключ из кэша редиса

	if err == nil {
		log.Printf("Ура! Ключ %s найден в кэше Redis. Статус: %s", key, cachedStatus)
		return &api.CheckKeyResponse{Status: cachedStatus}, nil // быстро возвращаем ответ
	}

	success, rdbError := s.rdb.SetNX(ctx, key) // Ставим лок на наш ключ
	if rdbError != nil {
		log.Printf("Ошибка распределенного замка Redis: %v", rdbError)
		return nil, rdbError
	}

	if !success { // Уже обрабатывается
		log.Printf("Запрос с ключом %s заблокирован: дубликат уже обрабатывается", key)
		return &api.CheckKeyResponse{Status: "pending"}, nil
	}

	status, err := s.db.IsKeyExists(ctx, key) // Проверка существования ключа в БД
	if err != nil {
		log.Printf("Ошибка при проверке ключа в Postgres: %v", err)
		return nil, err
	}

	switch status {
	case "success", "failed", "pending":
		log.Printf("Внимание: ключ %s НАЙДЕН в базе со статусом %s. Запрос отклонен.", key, status)
		return &api.CheckKeyResponse{Status: status}, nil

	case "not_found":
		// Ключ абсолютно новый. Бронируем его в Postgres.
		err = s.db.CreateKey(ctx, key)
		if err != nil {
			log.Printf("Не удалось забронировать ключ в БД: %v", err)
			return nil, err
		}

		log.Printf("Запрос с ключом %s допущен! Статус: not_found", key)
		return &api.CheckKeyResponse{Status: "not_found"}, nil
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

	err := s.db.SavePaymentResult(ctx, status, key, code, body)
	if err != nil {
		log.Printf("Не удалось сохранить результат в БД: %v", err)
		return nil, err
	}

	log.Printf("Результат платежа для ключа %s успешно сохранен в Postgres!", key)

	err = s.rdb.Set(ctx, key, status) // Сохраняем в кэш на сутки

	if err != nil {
		log.Printf("Не удалось сохранить статус в кэш Redis: %v", err)
	}

	return &api.ConfirmKeyResponse{
		Success: true,
	}, nil
}
