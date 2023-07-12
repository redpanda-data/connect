package io

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fileInputFieldPaths          = "paths"
	fileInputFieldCodec          = "codec"
	fileInputFieldMaxBuffer      = "max_buffer"
	fileInputFieldDeleteOnFinish = "delete_on_finish"
)

func fileInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Local").
		Summary(`Consumes data from files on disk, emitting messages according to a chosen codec.`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- path
- mod_time_unix
- mod_time (RFC3339)
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Example(
			"Read a Bunch of CSVs",
			"If we wished to consume a directory of CSV files as structured documents we can use a glob pattern and the `csv` codec:",
			`
input:
  file:
    paths: [ ./data/*.csv ]
    codec: csv
`,
		).
		Fields(
			service.NewStringListField(fileInputFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported, including super globs (double star)."),
			service.NewInternalField(codec.ReaderDocs).Default("lines"),
			service.NewIntField(fileInputFieldMaxBuffer).
				Description("The largest token size expected when consuming files with a tokenised codec such as `lines`.").
				Advanced().
				Default(1000000),
			service.NewBoolField(fileInputFieldDeleteOnFinish).
				Description("Whether to delete input files from the disk once they are fully consumed.").
				Advanced().
				Default(false),
		)
}

type fileInputConfig struct {
	Paths          []string
	Codec          string
	MaxBuffer      int
	DeleteOnFinish bool
}

func fileInputConfigFromParsed(pConf *service.ParsedConfig) (conf fileInputConfig, err error) {
	if conf.Paths, err = pConf.FieldStringList(fileInputFieldPaths); err != nil {
		return
	}
	if conf.Codec, err = pConf.FieldString(fileInputFieldCodec); err != nil {
		return
	}
	if conf.MaxBuffer, err = pConf.FieldInt(fileInputFieldMaxBuffer); err != nil {
		return
	}
	if conf.DeleteOnFinish, err = pConf.FieldBool(fileInputFieldDeleteOnFinish); err != nil {
		return
	}
	return
}

func init() {
	err := service.RegisterBatchInput("file", fileInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
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
			conf, err := fileInputConfigFromParsed(pConf)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			rdr, err := newFileConsumer(conf, mgr)
			if err != nil {
				return nil, err
			}

			i, err := input.NewAsyncReader("file", input.NewAsyncPreserver(rdr), mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type scannerInfo struct {
	scanner     codec.Reader
	currentPath string
	modTimeUTC  time.Time
}

type fileConsumer struct {
	log log.Modular
	nm  bundle.NewManagement

	paths       []string
	scannerCtor codec.ReaderConstructor

	scannerMut  sync.Mutex
	scannerInfo *scannerInfo

	delete bool
}

func newFileConsumer(conf fileInputConfig, nm bundle.NewManagement) (*fileConsumer, error) {
	expandedPaths, err := filepath.Globs(nm.FS(), conf.Paths)
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
		nm:          nm,
		log:         nm.Logger(),
		scannerCtor: ctor,
		paths:       expandedPaths,
		delete:      conf.DeleteOnFinish,
	}, nil
}

func (f *fileConsumer) Connect(ctx context.Context) error {
	return nil
}

func (f *fileConsumer) getReader(ctx context.Context) (scannerInfo, error) {
	f.scannerMut.Lock()
	defer f.scannerMut.Unlock()

	if f.scannerInfo != nil {
		return *f.scannerInfo, nil
	}

	if len(f.paths) == 0 {
		return scannerInfo{}, component.ErrTypeClosed
	}

	nextPath := f.paths[0]

	file, err := f.nm.FS().Open(nextPath)
	if err != nil {
		return scannerInfo{}, err
	}

	scanner, err := f.scannerCtor(nextPath, file, func(ctx context.Context, err error) error {
		if err == nil && f.delete {
			return f.nm.FS().Remove(nextPath)
		}
		return nil
	})
	if err != nil {
		file.Close()
		return scannerInfo{}, err
	}

	var modTimeUTC time.Time
	if fInfo, err := file.Stat(); err == nil {
		modTimeUTC = fInfo.ModTime().UTC()
	} else {
		f.log.Errorf("Failed to read metadata from file '%v'", nextPath)
	}

	f.scannerInfo = &scannerInfo{
		scanner:     scanner,
		currentPath: nextPath,
		modTimeUTC:  modTimeUTC,
	}

	f.paths = f.paths[1:]

	f.log.Infof("Consuming from file '%v'\n", nextPath)
	return *f.scannerInfo, nil
}

func (f *fileConsumer) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	for {
		scannerInfo, err := f.getReader(ctx)
		if err != nil {
			return nil, nil, err
		}

		parts, codecAckFn, err := scannerInfo.scanner.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				err = component.ErrTimeout
			}
			if err != component.ErrTimeout {
				scannerInfo.scanner.Close(ctx)
				f.scannerInfo = nil
			}
			if errors.Is(err, io.EOF) {
				continue
			}
			return nil, nil, err
		}

		msg := message.QuickBatch(nil)
		for _, part := range parts {
			if len(part.AsBytes()) == 0 {
				continue
			}

			part.MetaSetMut("path", scannerInfo.currentPath)
			part.MetaSetMut("mod_time_unix", scannerInfo.modTimeUTC.Unix())
			part.MetaSetMut("mod_time", scannerInfo.modTimeUTC.Format(time.RFC3339))

			msg = append(msg, part)
		}
		if msg.Len() == 0 {
			_ = codecAckFn(ctx, nil)
			return nil, nil, component.ErrTimeout
		}

		return msg, func(rctx context.Context, res error) error {
			return codecAckFn(rctx, res)
		}, nil
	}
}

func (f *fileConsumer) Close(ctx context.Context) (err error) {
	f.scannerMut.Lock()
	defer f.scannerMut.Unlock()

	if f.scannerInfo != nil {
		err = f.scannerInfo.scanner.Close(ctx)
		f.scannerInfo = nil
		f.paths = nil
	}
	return
}
