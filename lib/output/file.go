package output

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func init() {
	Constructors[TypeFile] = TypeSpec{
		constructor: fromSimpleConstructor(NewFile),
		Summary: `
Writes messages to files on disk based on a chosen codec.`,
		Description: `
Messages can be written to different files by using [interpolation functions](/docs/configuration/interpolation#bloblang-queries) in the path field. However, only one file is ever open at a given time, and therefore when the path changes the previously open file is closed.

` + multipartCodecDoc,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"path", "The file to write to, if the file does not yet exist it will be created.",
				"/tmp/data.txt",
				"/tmp/${! timestamp_unix() }.txt",
				`/tmp/${! json("document.id") }.json`,
			).IsInterpolated().AtVersion("3.33.0"),
			codec.WriterDocs.AtVersion("3.33.0"),
		},
		Categories: []Category{
			CategoryLocal,
		},
	}
}

//------------------------------------------------------------------------------

// FileConfig contains configuration fields for the file based output type.
type FileConfig struct {
	Path  string `json:"path" yaml:"path"`
	Codec string `json:"codec" yaml:"codec"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:  "",
		Codec: "lines",
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File output type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	f, err := newFileWriter(conf.File.Path, conf.File.Codec, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	w, err := NewAsyncWriter(TypeFile, 1, f, log, stats)
	if err != nil {
		return nil, err
	}
	if aw, ok := w.(*AsyncWriter); ok {
		aw.SetNoCancel()
	}
	return w, nil
}

//------------------------------------------------------------------------------

type fileWriter struct {
	log   log.Modular
	stats metrics.Type

	path      *field.Expression
	codec     codec.WriterConstructor
	codecConf codec.WriterConfig

	handleMut  sync.Mutex
	handlePath string
	handle     codec.Writer

	shutSig *shutdown.Signaller
}

func newFileWriter(pathStr, codecStr string, mgr types.Manager, log log.Modular, stats metrics.Type) (*fileWriter, error) {
	codec, codecConf, err := codec.GetWriter(codecStr)
	if err != nil {
		return nil, err
	}
	path, err := interop.NewBloblangField(mgr, pathStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %w", err)
	}
	return &fileWriter{
		codec:     codec,
		codecConf: codecConf,
		path:      path,
		log:       log,
		stats:     stats,
		shutSig:   shutdown.NewSignaller(),
	}, nil
}

//------------------------------------------------------------------------------

func (w *fileWriter) ConnectWithContext(ctx context.Context) error {
	return nil
}

func (w *fileWriter) WriteWithContext(ctx context.Context, msg types.Message) error {
	err := writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
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

	if msg.Len() > 1 {
		w.handleMut.Lock()
		if w.handle != nil {
			w.handle.EndBatch()
		}
		w.handleMut.Unlock()
	}
	return nil
}

// CloseAsync shuts down the File output and stops processing messages.
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

// WaitForClose blocks until the File output has closed down.
func (w *fileWriter) WaitForClose(timeout time.Duration) error {
	select {
	case <-w.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}
