package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func parquetDecodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Decodes https://parquet.apache.org/docs/[Parquet files^] into a batch of structured messages.").
		Field(service.NewBoolField("byte_array_as_string").
			Description("Whether to extract BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values as strings rather than byte slices in all cases. Values with a logical type of UTF8 will automatically be extracted as strings irrespective of this field. Enabling this field makes serializing the data as JSON more intuitive as `[]byte` values are serialized as base64 encoded strings by default.").
			Default(false).Deprecated()).
		Description(`
This processor uses https://github.com/parquet-go/parquet-go[https://github.com/parquet-go/parquet-go^], which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.`).
		Version("4.4.0").
		Example("Reading Parquet Files from AWS S3",
			"In this example we consume files from AWS S3 as they're written by listening onto an SQS queue for upload events. We make sure to use the `to_the_end` scanner which means files are read into memory in full, which then allows us to use a `parquet_decode` processor to expand each file into a batch of messages. Finally, we write the data out to local files as newline delimited JSON.",
			`
input:
  aws_s3:
    bucket: TODO
    prefix: foos/
    scanner:
      to_the_end: {}
    sqs:
      url: TODO
  processors:
    - parquet_decode: {}

output:
  file:
    codec: lines
    path: './foos/${! meta("s3_key") }.jsonl'
`)
}

func init() {
	err := service.RegisterProcessor(
		"parquet_decode", parquetDecodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newParquetDecodeProcessorFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newParquetDecodeProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*parquetDecodeProcessor, error) {
	return &parquetDecodeProcessor{
		logger: logger,
	}, nil
}

type parquetDecodeProcessor struct {
	logger *service.Logger
}

func newReaderWithoutPanic(r io.ReaderAt) (pRdr *parquet.GenericReader[any], err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("parquet read panic: %v", r)
		}
	}()

	pRdr = parquet.NewGenericReader[any](r)
	return
}

func readWithoutPanic(pRdr *parquet.GenericReader[any], rows []any) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("decoding panic: %v", r)
		}
	}()

	n, err = pRdr.Read(rows)
	return
}

func (s *parquetDecodeProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	mBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	inFile, err := parquet.OpenFile(bytes.NewReader(mBytes), int64(len(mBytes)))
	if err != nil {
		return nil, err
	}

	pRdr, err := newReaderWithoutPanic(inFile)
	if err != nil {
		return nil, err
	}

	rowBuf := make([]any, 10)
	var resBatch service.MessageBatch

	for {
		n, err := readWithoutPanic(pRdr, rowBuf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			newMsg := msg.Copy()
			newMsg.SetStructuredMut(rowBuf[i])
			resBatch = append(resBatch, newMsg)
		}
	}

	return resBatch, nil
}

func (s *parquetDecodeProcessor) Close(ctx context.Context) error {
	return nil
}
