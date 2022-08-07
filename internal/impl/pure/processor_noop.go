package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(c processor.Config, nm bundle.NewManagement) (processor.V1, error) {
		return &noopProcessor{}, nil
	}, docs.ComponentSpec{
		Name:    "noop",
		Summary: "Noop is a processor that does nothing, the message passes through unchanged. Why? Sometimes doing nothing is the braver option.",
		Config:  docs.FieldObject("", "").HasDefault(struct{}{}),
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
