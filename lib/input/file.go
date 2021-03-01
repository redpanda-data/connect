package input

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/filepath"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeFile] = TypeSpec{
		constructor: fromSimpleConstructor(NewFile),
		Summary: `
Consumes data from files on disk, emitting messages according to a chosen codec.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("paths", "A list of paths to consume sequentially. Glob patterns are supported.").Array(),
			codec.ReaderDocs,
			docs.FieldAdvanced("max_buffer", "The largest token size expected when consuming delimited files."),
			docs.FieldDeprecated("path"),
			docs.FieldDeprecated("delimiter"),
			docs.FieldDeprecated("multipart"),
			docs.FieldAdvanced("delete_on_finish", "Whether to delete consumed files from the disk once they are fully consumed."),
		},
		Description: `
### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- path
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#metadata).`,
		Categories: []Category{
			CategoryLocal,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Read a Bunch of CSVs",
				Summary: "If we wished to consume a directory of CSV files as structured documents we can use a glob pattern and the `csv` codec:",
				Config: `
input:
  file:
    paths: [ ./data/*.csv ]
    codec: csv
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// FileConfig contains configuration values for the File input type.
type FileConfig struct {
	Path           string   `json:"path" yaml:"path"`
	Paths          []string `json:"paths" yaml:"paths"`
	Codec          string   `json:"codec" yaml:"codec"`
	Multipart      bool     `json:"multipart" yaml:"multipart"`
	MaxBuffer      int      `json:"max_buffer" yaml:"max_buffer"`
	Delim          string   `json:"delimiter" yaml:"delimiter"`
	DeleteOnFinish bool     `json:"delete_on_finish" yaml:"delete_on_finish"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Path:  "",
		Paths: []string{},
		// TODO: V4 change this default
		Codec:          "lines",
		Multipart:      false,
		MaxBuffer:      1000000,
		Delim:          "",
		DeleteOnFinish: false,
	}
}

//------------------------------------------------------------------------------

// NewFile creates a new File input type.
func NewFile(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	if len(conf.File.Path) > 0 {
		conf.File.Paths = append(conf.File.Paths, conf.File.Path)
	}
	if len(conf.File.Delim) > 0 {
		conf.File.Codec = "delim:" + conf.File.Delim
	}
	if conf.File.Multipart && !strings.HasSuffix(conf.File.Codec, "/multipart") {
		conf.File.Codec = conf.File.Codec + "/multipart"
	}
	rdr, err := newFileConsumer(conf.File, log)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeFile, true, reader.NewAsyncPreserver(rdr), log, stats)
}

//------------------------------------------------------------------------------

type fileConsumer struct {
	log log.Modular

	paths       []string
	scannerCtor codec.ReaderConstructor

	scannerMut  sync.Mutex
	scanner     codec.Reader
	currentPath string

	delete bool
}

func newFileConsumer(conf FileConfig, log log.Modular) (*fileConsumer, error) {
	expandedPaths, err := filepath.Globs(conf.Paths)
	if err != nil {
		return nil, err
	}

	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	return &fileConsumer{
		log:         log,
		scannerCtor: ctor,
		paths:       expandedPaths,
		delete:      conf.DeleteOnFinish,
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (f *fileConsumer) ConnectWithContext(ctx context.Context) error {
	f.scannerMut.Lock()
	defer f.scannerMut.Unlock()

	if f.scanner != nil {
		return nil
	}

	if len(f.paths) == 0 {
		return types.ErrTypeClosed
	}

	nextPath := f.paths[0]

	file, err := os.Open(nextPath)
	if err != nil {
		return err
	}

	if f.scanner, err = f.scannerCtor(nextPath, file, func(ctx context.Context, err error) error {
		if err == nil && f.delete {
			return os.Remove(nextPath)
		}
		return nil
	}); err != nil {
		file.Close()
		return err
	}

	f.currentPath = nextPath
	f.paths = f.paths[1:]

	f.log.Infof("Consuming from file '%v'\n", nextPath)
	return nil
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (f *fileConsumer) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	f.scannerMut.Lock()
	defer f.scannerMut.Unlock()

	if f.scanner == nil {
		return nil, nil, types.ErrNotConnected
	}

	parts, codecAckFn, err := f.scanner.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = types.ErrTimeout
		}
		if err != types.ErrTimeout {
			f.scanner.Close(ctx)
			f.scanner = nil
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, types.ErrTimeout
		}
		return nil, nil, err
	}

	msg := message.New(nil)
	for _, part := range parts {
		if len(part.Get()) > 0 {
			part.Metadata().Set("path", f.currentPath)
			msg.Append(part)
		}
	}
	if msg.Len() == 0 {
		codecAckFn(ctx, nil)
		return nil, nil, types.ErrTimeout
	}

	return msg, func(rctx context.Context, res types.Response) error {
		return codecAckFn(rctx, res.Error())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (f *fileConsumer) CloseAsync() {
	go func() {
		f.scannerMut.Lock()
		if f.scanner != nil {
			f.scanner.Close(context.Background())
			f.scanner = nil
			f.paths = nil
		}
		f.scannerMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (f *fileConsumer) WaitForClose(time.Duration) error {
	return nil
}
