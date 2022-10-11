package io

import (
	"context"
	"os"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		f, err := newStdoutWriter(conf.STDOUT.Codec)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("stdout", 1, f, nm)
		if err != nil {
			return nil, err
		}
		return w, nil
	}), docs.ComponentSpec{
		Name: "stdout",
		Summary: `
Prints messages to stdout as a continuous stream of data, dividing messages according to the specified codec.`,
		Config: docs.FieldComponent().WithChildren(
			codec.WriterDocs.AtVersion("3.46.0").HasDefault("lines"),
		),
		Categories: []string{
			"Local",
		},
	})
	if err != nil {
		panic(err)
	}
}

type stdoutWriter struct {
	handle codec.Writer
}

func newStdoutWriter(codecStr string) (*stdoutWriter, error) {
	codec, _, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}

	handle, err := codec(os.Stdout)
	if err != nil {
		return nil, err
	}

	return &stdoutWriter{
		handle: handle,
	}, nil
}

func (w *stdoutWriter) Connect(ctx context.Context) error {
	return nil
}

func (w *stdoutWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		return w.handle.Write(ctx, p)
	})
}

func (w *stdoutWriter) Close(ctx context.Context) error {
	return nil
}
