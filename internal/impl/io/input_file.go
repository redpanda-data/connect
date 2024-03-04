package io

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fileInputFieldPaths          = "paths"
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
			"If we wished to consume a directory of CSV files as structured documents we can use a glob pattern and the `csv` scanner:",
			`
input:
  file:
    paths: [ ./data/*.csv ]
    scanner:
      csv: {}
`,
		).
		Fields(
			service.NewStringListField(fileInputFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported, including super globs (double star)."),
		).
		Fields(interop.OldReaderCodecFields("lines")...).
		Fields(
			service.NewBoolField(fileInputFieldDeleteOnFinish).
				Description("Whether to delete input files from the disk once they are fully consumed.").
				Advanced().
				Default(false),
		)
}

func init() {
	err := service.RegisterBatchInput("file", fileInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			r, err := fileConsumerFromParsed(pConf, res)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(r), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type scannerInfo struct {
	scanner     interop.FallbackReaderStream
	currentPath string
	modTimeUTC  time.Time
}

type fileConsumer struct {
	log *service.Logger
	nm  *service.Resources

	paths       []string
	scannerCtor interop.FallbackReaderCodec

	scannerMut  sync.Mutex
	scannerInfo *scannerInfo

	delete bool
}

func fileConsumerFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fileConsumer, error) {
	paths, err := conf.FieldStringList(fileInputFieldPaths)
	if err != nil {
		return nil, err
	}

	deleteOnFinish, err := conf.FieldBool(fileInputFieldDeleteOnFinish)
	if err != nil {
		return nil, err
	}

	expandedPaths, err := filepath.Globs(nm.FS(), paths)
	if err != nil {
		return nil, err
	}

	ctor, err := interop.OldReaderCodecFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return &fileConsumer{
		nm:          nm,
		log:         nm.Logger(),
		scannerCtor: ctor,
		paths:       expandedPaths,
		delete:      deleteOnFinish,
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

	details := scanner.SourceDetails{
		Name: nextPath,
	}

	scanner, err := f.scannerCtor.Create(file, func(ctx context.Context, err error) error {
		if err == nil && f.delete {
			return f.nm.FS().Remove(nextPath)
		}
		return nil
	}, details)
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

func (f *fileConsumer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		scannerInfo, err := f.getReader(ctx)
		if err != nil {
			return nil, nil, err
		}

		parts, codecAckFn, err := scannerInfo.scanner.NextBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				err = component.ErrTimeout
			}
			if err != component.ErrTimeout {
				f.scannerMut.Lock()
				scannerInfo.scanner.Close(ctx)
				f.scannerInfo = nil
				f.scannerMut.Unlock()
			}
			if errors.Is(err, io.EOF) {
				continue
			}
			return nil, nil, err
		}

		for _, part := range parts {
			part.MetaSetMut("path", scannerInfo.currentPath)
			part.MetaSetMut("mod_time_unix", scannerInfo.modTimeUTC.Unix())
			part.MetaSetMut("mod_time", scannerInfo.modTimeUTC.Format(time.RFC3339))
		}

		if len(parts) == 0 {
			_ = codecAckFn(ctx, nil)
			return nil, nil, component.ErrTimeout
		}

		return parts, func(rctx context.Context, res error) error {
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
