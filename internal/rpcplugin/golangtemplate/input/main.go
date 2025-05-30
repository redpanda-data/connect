package main

import (
	"context"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/plugin/go/rpcn"
)

type config struct {
}

func main() {
	rpcn.InputMain(func(cfg config) (input service.BatchInput, autoRetryNacks bool, err error) {
		input = &myInput{cfg: cfg}
		autoRetryNacks = true
		return
	})
}

type myInput struct {
	cfg      config
	messages service.MessageBatch
}

var _ service.BatchInput = (*myInput)(nil)

// Connect implements service.BatchInput.
func (m *myInput) Connect(context.Context) error {
	m.messages = service.MessageBatch{
		service.NewMessage([]byte("hello")),
		service.NewMessage([]byte("world")),
		service.NewMessage([]byte("!")),
	}
	return nil
}

// ReadBatch implements service.BatchInput.
func (m *myInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	if len(m.messages) == 0 {
		return nil, nil, service.ErrEndOfInput
	}
	msg := m.messages[0]
	m.messages = m.messages[1:]
	return service.MessageBatch{msg}, noopAck, nil
}

// Close implements service.BatchInput.
func (m *myInput) Close(context.Context) error {
	return nil
}

// This is a no-op ack function, we can ignore the error because we have autoRetryNacks set to true.
func noopAck(ctx context.Context, err error) error {
	return nil
}
