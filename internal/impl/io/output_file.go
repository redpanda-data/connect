package io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		f, err := newFileWriter(conf.File.Path, conf.File.Codec, nm)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("file", 1, f, nm)
		if err != nil {
			return nil, err
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
	nm  bundle.NewManagement

	path      *field.Expression
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	handleMut  sync.Mutex
	handlePath string
	handle     codec.Writer
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
		nm:        mgr,
	}, nil
}

//------------------------------------------------------------------------------

func (w *fileWriter) Connect(ctx context.Context) error {
	return nil
}

func (w *fileWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	err := output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		path, err := w.path.String(i, msg)
		if err != nil {
			return fmt.Errorf("path interpolation error: %w", err)
		}
		path = filepath.Clean(path)

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

		if err := w.nm.FS().MkdirAll(filepath.Dir(path), fs.FileMode(0o777)); err != nil {
			return err
		}

		file, err := w.nm.FS().OpenFile(path, flag, fs.FileMode(0o666))
		if err != nil {
			return err
		}

		fileWriter, ok := file.(io.WriteCloser)
		if !ok {
			_ = file.Close()
			return errors.New("failed to open file for writing")
		}

		w.handlePath = path
		handle, err := w.codec(fileWriter)
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

func (w *fileWriter) Close(ctx context.Context) error {
	w.handleMut.Lock()
	defer w.handleMut.Unlock()

	var err error
	if w.handle != nil {
		err = w.handle.Close(ctx)
		w.handle = nil
	}
	return err
}
