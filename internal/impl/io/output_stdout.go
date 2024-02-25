package io

import (
	"context"
	"io"
	"os"

	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterOutput(
		"stdout", service.NewConfigSpec().
			Stable().
			Categories("Local").
			Summary(`Prints messages to stdout as a continuous stream of data.`).
			Fields(service.NewInternalField(codec.NewWriterDocs("codec").AtVersion("3.46.0").HasDefault("lines"))),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			w, err := newStdoutWriterFromParsed(conf)
			if err != nil {
				return nil, 0, err
			}
			return w, 1, nil
		})
	if err != nil {
		panic(err)
	}
}

type stdoutWriter struct {
	suffixFn codec.SuffixFn
	handle   io.WriteCloser
}

func newStdoutWriterFromParsed(conf *service.ParsedConfig) (*stdoutWriter, error) {
	codecStr, err := conf.FieldString("codec")
	if err != nil {
		return nil, err
	}

	codec, _, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}

	return &stdoutWriter{
		suffixFn: codec,
		handle:   os.Stdout,
	}, nil
}

func (w *stdoutWriter) Connect(ctx context.Context) error {
	return nil
}

func (w *stdoutWriter) writeTo(wtr io.Writer, p *service.Message) error {
	mBytes, err := p.AsBytes()
	if err != nil {
		return err
	}

	suffix, addSuffix := w.suffixFn(mBytes)

	if _, err := wtr.Write(mBytes); err != nil {
		return err
	}
	if addSuffix {
		if _, err := wtr.Write(suffix); err != nil {
			return err
		}
	}
	return nil
}

func (w *stdoutWriter) Write(ctx context.Context, msg *service.Message) error {
	return w.writeTo(w.handle, msg)
}

func (w *stdoutWriter) Close(ctx context.Context) error {
	return nil
}
