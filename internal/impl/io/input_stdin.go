package io

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		rdr, err := newStdinConsumer(conf.STDIN)
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("stdin", input.NewAsyncCutOff(input.NewAsyncPreserver(rdr)), nm)
	}), docs.ComponentSpec{
		Name:    "stdin",
		Summary: `Consumes data piped to stdin as line delimited messages.`,
		Description: `
If the multipart option is set to true then lines are interpretted as message parts, and an empty line indicates the end of the message.

If the delimiter field is left empty then line feed (\n) is used.`,
		Config: docs.FieldComponent().WithChildren(
			codec.ReaderDocs.AtVersion("3.42.0"),
			docs.FieldInt("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewSTDINConfig()),
		Categories: []string{
			"Local",
		},
	})
	if err != nil {
		panic(err)
	}
}

type stdinConsumer struct {
	scanner codec.Reader
}

func newStdinConsumer(conf input.STDINConfig) (*stdinConsumer, error) {
	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	scanner, err := ctor("", io.NopCloser(os.Stdin), func(_ context.Context, err error) error {
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &stdinConsumer{scanner}, nil
}

func (s *stdinConsumer) Connect(ctx context.Context) error {
	return nil
}

func (s *stdinConsumer) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	parts, codecAckFn, err := s.scanner.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			s.scanner.Close(ctx)
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, component.ErrTypeClosed
		}
		return nil, nil, err
	}
	_ = codecAckFn(ctx, nil)

	msg := message.Batch(parts)
	if msg.Len() == 0 {
		return nil, nil, component.ErrTimeout
	}

	return msg, func(rctx context.Context, res error) error {
		return nil
	}, nil
}

func (s *stdinConsumer) Close(ctx context.Context) (err error) {
	if s.scanner != nil {
		err = s.scanner.Close(ctx)
	}
	return
}
