package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchOutput(
		"drop", service.NewConfigSpec().
			Stable().
			Categories("Utility").
			Summary(`Drops all messages.`).
			Field(service.NewObjectField("").Default(map[string]any{})),
		func(conf *service.ParsedConfig, res *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			nm := interop.UnwrapManagement(res)
			var o output.Streamed
			if o, err = output.NewAsyncWriter("drop", 1, newDropWriter(nm.Logger()), nm); err != nil {
				return
			}
			out = interop.NewUnwrapInternalOutput(o)
			return
		})
	if err != nil {
		panic(err)
	}
}

type dropWriter struct {
	log log.Modular
}

func newDropWriter(log log.Modular) *dropWriter {
	return &dropWriter{log: log}
}

func (d *dropWriter) Connect(ctx context.Context) error {
	return nil
}

func (d *dropWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	return nil
}

func (d *dropWriter) Close(context.Context) error {
	return nil
}
