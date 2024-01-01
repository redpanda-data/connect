package pure

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

const scFieldSize = "size"

func chunkerScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Split an input stream into chunks of a given number of bytes.").
		Fields(
			service.NewIntField(scFieldSize).
				Description("The size of each chunk in bytes."),
		)
}

func init() {
	err := service.RegisterBatchScannerCreator("chunker", chunkerScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return chunkerScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func chunkerScannerFromParsed(conf *service.ParsedConfig) (l *chunkerScannerCreator, err error) {
	l = &chunkerScannerCreator{}
	if l.size, err = conf.FieldInt(scFieldSize); err != nil {
		return
	}
	return
}

type chunkerScannerCreator struct {
	size int
}

func (c *chunkerScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	return service.AutoAggregateBatchScannerAcks(&chunkerScanner{
		r:    rdr,
		size: int64(c.size),
		buf:  bytes.NewBuffer(make([]byte, 0, c.size)),
	}, aFn), nil
}

func (c *chunkerScannerCreator) Close(context.Context) error {
	return nil
}

type chunkerScanner struct {
	size int64
	buf  *bytes.Buffer
	r    io.ReadCloser
}

func (c *chunkerScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if c.r == nil {
		return nil, io.EOF
	}

	_, err := io.CopyN(c.buf, c.r, c.size)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if c.buf.Len() == 0 {
		return nil, io.EOF
	}

	bytesCopy := make([]byte, c.buf.Len())
	copy(bytesCopy, c.buf.Bytes())

	c.buf.Reset()
	if err != nil {
		_ = c.r.Close()
		c.r = nil
	}
	return service.MessageBatch{service.NewMessage(bytesCopy)}, nil
}

func (c *chunkerScanner) Close(ctx context.Context) error {
	if c.r == nil {
		return nil
	}
	return c.r.Close()
}
