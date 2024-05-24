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

	"github.com/benthosdev/benthos/v4/internal/codec"
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
		Description(`Messages can be written to different files by using xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions] in the path field. However, only one file is ever open at a given time, and therefore when the path changes the previously open file is closed.`).
		Fields(
			service.NewInterpolatedStringField(fileOutputFieldPath).
				Description("The file to write to, if the file does not yet exist it will be created.").
				Examples(
					"/tmp/data.txt",
					"/tmp/${! timestamp_unix() }.txt",
					`/tmp/${! json("document.id") }.json`,
				).
				Version("3.33.0"),
			service.NewInternalField(codec.NewWriterDocs(fileOutputFieldCodec)).Version("3.33.0").Default("lines"),
		)
}

type fileOutputConfig struct {
	Path  *service.InterpolatedString
	Codec string
}

func fileOutputConfigFromParsed(pConf *service.ParsedConfig) (conf fileOutputConfig, err error) {
	if conf.Path, err = pConf.FieldInterpolatedString(fileOutputFieldPath); err != nil {
		return
	}
	if conf.Codec, err = pConf.FieldString(fileOutputFieldCodec); err != nil {
		return
	}
	return
}

func init() {
	err := service.RegisterOutput("file", fileOutputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (out service.Output, mif int, err error) {
			var conf fileOutputConfig
			if conf, err = fileOutputConfigFromParsed(pConf); err != nil {
				return
			}

			mif = 1
			out, err = newFileWriter(conf.Path, conf.Codec, res)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type fileWriter struct {
	log *service.Logger
	nm  *service.Resources

	path       *service.InterpolatedString
	suffixFn   codec.SuffixFn
	appendMode bool

	handleMut  sync.Mutex
	handlePath string
	handle     io.WriteCloser
}

func newFileWriter(path *service.InterpolatedString, codecStr string, mgr *service.Resources) (*fileWriter, error) {
	codec, appendMode, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}
	return &fileWriter{
		suffixFn:   codec,
		appendMode: appendMode,
		path:       path,
		log:        mgr.Logger(),
		nm:         mgr,
	}, nil
}

//------------------------------------------------------------------------------

func (w *fileWriter) Connect(ctx context.Context) error {
	return nil
}

func (w *fileWriter) writeTo(wtr io.Writer, p *service.Message) error {
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

func (w *fileWriter) Write(ctx context.Context, msg *service.Message) error {
	path, err := w.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %w", err)
	}
	path = filepath.Clean(path)

	w.handleMut.Lock()
	defer w.handleMut.Unlock()

	if w.handle != nil && path == w.handlePath {
		return w.writeTo(w.handle, msg)
	}
	if w.handle != nil {
		if err := w.handle.Close(); err != nil {
			return err
		}
	}

	flag := os.O_CREATE | os.O_RDWR
	if w.appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	if err := w.nm.FS().MkdirAll(filepath.Dir(path), fs.FileMode(0o777)); err != nil {
		return err
	}

	file, err := w.nm.FS().OpenFile(path, flag, fs.FileMode(0o666))
	if err != nil {
		return err
	}

	handle, ok := file.(io.WriteCloser)
	if !ok {
		_ = file.Close()
		return errors.New("failed to open file for writing")
	}

	w.handlePath = path
	if err := w.writeTo(handle, msg); err != nil {
		_ = handle.Close()
		return err
	}

	if w.appendMode {
		w.handle = handle
	} else {
		_ = handle.Close()
	}
	return nil
}

func (w *fileWriter) Close(ctx context.Context) error {
	w.handleMut.Lock()
	defer w.handleMut.Unlock()

	var err error
	if w.handle != nil {
		err = w.handle.Close()
		w.handle = nil
	}
	return err
}
