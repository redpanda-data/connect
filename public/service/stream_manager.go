package service

import (
	"context"
	"errors"
	"time"

	"github.com/benthosdev/benthos/v4/internal/stream/manager"
)

var ErrStreamManagerNotReady = errors.New("Stream Manager is not ready")

func IsStreamManagerReady() bool {
	return manager.Singleton != nil
}

func HandleStreamCRUD(method, id, streamConfig string, timeout time.Duration) error {
	if !IsStreamManagerReady() {
		return ErrStreamManagerNotReady
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return manager.Singleton.HandleDirectStreamCRUD(ctx, method, id, streamConfig)
}
