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
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fileOutputFieldPath  = "path"
	fileOutputFieldCodec = "codec"
)

func fileOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Local").
		Summary(`Writes messages to files on disk based on a chosen codec.`).
		Description(`Messages can be written to different files by using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the path field. However, only one file is ever open at a given time, and therefore when the path changes the previously open file is closed.`).
		Fields(
			service.NewInterpolatedStringField(fileOutputFieldPath).
				Description("The file to write to, if the file does not yet exist it will be created.").
				Examples(
					"/tmp/data.txt",
					"/tmp/${! timestamp_unix() }.txt",
					`/tmp/${! json("document.id") }.json`,
				).
				Version("3.33.0"),
			service.NewInternalField(codec.WriterDocs).Version("3.33.0").Default("lines"),
		)
}

type fileOutputConfig struct {
	Path  string
	Codec string
}

func fileOutputConfigFromParsed(pConf *service.ParsedConfig) (conf fileOutputConfig, err error) {
	if conf.Path, err = pConf.FieldString(fileOutputFieldPath); err != nil {
		return
	}
	if conf.Codec, err = pConf.FieldString(fileOutputFieldCodec); err != nil {
		return
	}
	return
}

func init() {
	err := service.RegisterBatchOutput("file", fileOutputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (out service.BatchOutput, pol service.BatchPolicy, mif int, err error) {
			// NOTE: We're using interop to punch an internal implementation up
			// to the public plugin API. The only blocker from using the full
			// public suite is the codec field.
			//
			// Since codecs are likely to get refactored soon I figured it
			// wasn't worth investing in a public wrapper since the old style
			// will likely get deprecated.
			//
			// This does mean that for now all codec based components will need
			// to keep internal implementations. However, the config specs are
			// the biggest time sink when converting to the new APIs so it's not
			// a big deal to leave these tasks pending.
			var conf fileOutputConfig
			if conf, err = fileOutputConfigFromParsed(pConf); err != nil {
				return
			}

			mgr := interop.UnwrapManagement(res)
			var f *fileWriter
			if f, err = newFileWriter(conf.Path, conf.Codec, mgr); err != nil {
				return
			}

			var w output.Streamed
			if w, err = output.NewAsyncWriter("file", 1, f, mgr); err != nil {
				return
			}

			out = interop.NewUnwrapInternalOutput(w)
			return
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
