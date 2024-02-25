package pure

import (
	"context"
	"io"

	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	sdFieldAlgorithm = "algorithm"
	sdFieldChild     = "into"
)

func decompressScannerSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Decompress the stream of bytes according to an algorithm, before feeding it into a child scanner.").
		Fields(
			service.NewStringField(sdFieldAlgorithm).
				Description("One of `gzip`, `pgzip`, `zlib`, `bzip2`, `flate`, `snappy`, `lz4`, `zstd`."),
			service.NewScannerField(sdFieldChild).
				Description("The child scanner to feed the decompressed stream into.").
				Default(map[string]any{"to_the_end": map[string]any{}}),
		)
}

func init() {
	err := service.RegisterBatchScannerCreator("decompress", decompressScannerSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchScannerCreator, error) {
			return decompressScannerFromParsed(conf)
		})
	if err != nil {
		panic(err)
	}
}

func decompressScannerFromParsed(conf *service.ParsedConfig) (l *decompressScannerCreator, err error) {
	l = &decompressScannerCreator{}
	var decompAlg string
	if decompAlg, err = conf.FieldString(sdFieldAlgorithm); err != nil {
		return
	}
	if l.decompReaderCtor, err = strToDecompressReader(decompAlg); err != nil {
		return
	}
	if l.child, err = conf.FieldScanner(sdFieldChild); err != nil {
		return
	}
	return
}

type decompressScannerCreator struct {
	decompReaderCtor DecompressReader
	child            *service.OwnedScannerCreator
}

func (c *decompressScannerCreator) Create(rdr io.ReadCloser, aFn service.AckFunc, details *service.ScannerSourceDetails) (service.BatchScanner, error) {
	dRdr, err := c.decompReaderCtor(rdr)
	if err != nil {
		return nil, err
	}
	cRdr, ok := dRdr.(io.ReadCloser)
	if !ok {
		cRdr = io.NopCloser(dRdr)
	}
	return c.child.Create(cRdr, aFn, details)
}

func (c *decompressScannerCreator) Close(context.Context) error {
	return nil
}
