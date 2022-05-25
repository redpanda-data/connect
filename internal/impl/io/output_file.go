package io

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		f, err := newFileWriter(conf.File.Path, conf.File.Codec, nm)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("file", 1, f, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		if aw, ok := w.(*output.AsyncWriter); ok {
			aw.SetNoCancel()
		}
		return w, nil
	}), docs.ComponentSpec{
		Name: "file",
		Summary: `
Writes messages to files on disk based on a chosen codec.`,
		Description: `Messages can be written to different files by using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the path field. However, only one file is ever open at a given time, and therefore when the path changes the previously open file is closed.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"path", "The file to write to, if the file does not yet exist it will be created.",
				"/tmp/data.txt",
				"/tmp/${! timestamp_unix() }.txt",
				`/tmp/${! json("document.id") }.json`,
			).IsInterpolated().AtVersion("3.33.0"),
			codec.WriterDocs.AtVersion("3.33.0"),
		).ChildDefaultAndTypesFromStruct(output.NewFileConfig()),
		Categories: []string{
			"Local",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type fileWriter struct {
	log log.Modular

	path      *field.Expression
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	handleMut  sync.Mutex
	handlePath string
	handle     codec.Writer

	shutSig *shutdown.Signaller
}

func newFileWriter(pathStr, codecStr string, mgr bundle.NewManagement) (*fileWriter, error) {
	codec, codecConf, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}
	path, err := mgr.BloblEnvironment().NewField(pathStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %w", err)
	}
	return &fileWriter{
		codec:     codec,
		codecConf: codecConf,
		path:      path,
		log:       mgr.Logger(),
		shutSig:   shutdown.NewSignaller(),
	}, nil
}

//------------------------------------------------------------------------------

func (w *fileWriter) ConnectWithContext(ctx context.Context) error {
	return nil
}

func (w *fileWriter) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	err := output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		path := filepath.Clean(w.path.String(i, msg))

		w.handleMut.Lock()
		defer w.handleMut.Unlock()

		if w.handle != nil && path == w.handlePath {
			return w.handle.Write(ctx, p)
		}
		if w.handle != nil {
			if err := w.handle.Close(ctx); err != nil {
				return err
			}
		}

		flag := os.O_CREATE | os.O_RDWR
		if w.codecConf.Append {
			flag |= os.O_APPEND
		}
		if w.codecConf.Truncate {
			flag |= os.O_TRUNC
		}

		if err := os.MkdirAll(filepath.Dir(path), os.FileMode(0o777)); err != nil {
			return err
		}

		file, err := os.OpenFile(path, flag, os.FileMode(0o666))
		if err != nil {
			return err
		}

		w.handlePath = path
		handle, err := w.codec(file)
		if err != nil {
			return err
		}

		if err = handle.Write(ctx, p); err != nil {
			handle.Close(ctx)
			return err
		}

		if !w.codecConf.CloseAfter {
			w.handle = handle
		} else {
			handle.Close(ctx)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (w *fileWriter) CloseAsync() {
	go func() {
		w.handleMut.Lock()
		if w.handle != nil {
			w.handle.Close(context.Background())
			w.handle = nil
		}
		w.handleMut.Unlock()
		w.shutSig.ShutdownComplete()
	}()
}

func (w *fileWriter) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return component.ErrTimeout
	}
	return nil
}
