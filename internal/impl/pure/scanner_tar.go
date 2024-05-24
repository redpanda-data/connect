package pure

import (
	"archive/tar"
	"bytes"
	"context"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

func tarScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Consume a tar archive file by file.").
		Description(`
== Metadata

This scanner adds the following metadata to each message:

- ` + "`tar_name`" + `

`).
		Field(service.NewObjectField("").Default(map[string]any{}))
}

func init() {
	err := service.RegisterBatchScannerCreator("tar", tarScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return tarScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func tarScannerFromParsed(conf *service.ParsedConfig) (l *tarScannerCreator, err error) {
	l = &tarScannerCreator{}
	return
}

type tarScannerCreator struct{}

func (c *tarScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	return service.AutoAggregateBatchScannerAcks(&tarScanner{
		r: rdr,
		t: tar.NewReader(rdr),
	}, aFn), nil
}

func (c *tarScannerCreator) Close(context.Context) error {
	return nil
}

type tarScanner struct {
	t *tar.Reader
	r io.ReadCloser
}

func (c *tarScanner) NextBatch(ctx context.Context) (service.MessageBatch, error) {
	if c.r == nil {
		return nil, io.EOF
	}

	hdr, err := c.t.Next()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, c.t); err != nil {
		return nil, err
	}

	msg := service.NewMessage(buf.Bytes())
	msg.MetaSetMut("tar_name", hdr.Name)

	return service.MessageBatch{msg}, nil
}

func (c *tarScanner) Close(ctx context.Context) error {
	if c.r == nil {
		return nil
	}
	return c.r.Close()
}
