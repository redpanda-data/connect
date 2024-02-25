package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchProcessor("noop", service.NewConfigSpec().
		Stable().
		Summary("Noop is a processor that does nothing, the message passes through unchanged. Why? Sometimes doing nothing is the braver option.").
		Field(service.NewObjectField("").Default(map[string]any{})),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			p := &noopProcessor{}
			return interop.NewUnwrapInternalBatchProcessor(p), nil
		})
	if err != nil {
		panic(err)
	}
}

type noopProcessor struct{}

func (c *noopProcessor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	msgs := [1]message.Batch{msg}
	return msgs[:], nil
}

func (c *noopProcessor) Close(context.Context) error {
	return nil
}
