package main

import (
	"bytes"
	"context"
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/plugin/go/rpcn"
)

type config struct {
	Suffix string
}

func main() {
	rpcn.ProcessorMain(func(cfg config) (service.BatchProcessor, error) {
		return &myProcessor{suffix: []byte(cfg.Suffix)}, nil
	})
}

type myProcessor struct {
	suffix []byte
}

var _ service.BatchProcessor = (*myProcessor)(nil)

// ProcessBatch implements service.BatchProcessor.
func (p *myProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	for _, m := range batch {
		mBytes, err := m.AsBytes()
		if err != nil {
			return nil, err
		}
		m.SetBytes(slices.Concat(
			[]byte("MEOW! "),
			bytes.ToUpper(mBytes),
			p.suffix,
		))
	}
	return []service.MessageBatch{batch}, nil
}

// Close implements service.BatchProcessor.
func (*myProcessor) Close(context.Context) error {
	return nil
}
