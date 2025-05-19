// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

const (
	pFieldByteArrayAsString  = "byte_array_as_string"
	pFieldHandleLogicalTypes = "handle_logical_types"
)

func parquetDecodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Decodes https://parquet.apache.org/docs/[Parquet files^] into a batch of structured messages.").
		Field(service.NewBoolField(pFieldByteArrayAsString).
			Description("Whether to extract BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values as strings rather than byte slices in all cases. Values with a logical type of UTF8 will automatically be extracted as strings irrespective of this field. Enabling this field makes serializing the data as JSON more intuitive as `[]byte` values are serialized as base64 encoded strings by default.").
			Default(false).Deprecated()).
		Field(service.NewStringAnnotatedEnumField(pFieldHandleLogicalTypes, map[string]string{
			"v1": "No special handling of logical types",
			"v2": `
- TIMESTAMP - decodes as an RFC3339 string describing the time. If the ` + "`isAdjustedToUTC`" + ` flag is set to true in the parquet file, the time zone will be set to UTC. If it is set to false the time zone will be set to local time.
- UUID - decodes as a string, i.e. ` + "`00112233-4455-6677-8899-aabbccddeeff`" + `.`,
		}).
			Description("Whether to be smart about decoding logical types. In the Parquet format, logical types are stored as one of the standard physical types with some additional metadata describing the logical type. For example, UUIDs are stored in a FIXED_LEN_BYTE_ARRAY physical type, but there is metadata in the schema denoting that it is a UUID. By default, this logical type metadata will be ignored and values will be decoded directly from the physical type, which isn't always desirable. By enabling this option, logical types will be given special treatment and will decode into more useful values. The value for this field specifies a version, i.e. v0, v1... Any given version enables the logical type handling for that version and all versions below it, which allows the handling of new logical types to be introduced without breaking existing pipelines. We recommend enabling the newest version available of this feature when creating new pipelines.").
			Example("v2").
			Default("v1")). // TODO: V5 bump this to the latest version
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
	service.MustRegisterProcessor(
		"parquet_decode", parquetDecodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newParquetDecodeProcessorFromConfig(conf, mgr.Logger())
		})
}

//------------------------------------------------------------------------------

const (
	logicalTypesVersionV1 = "v1"
	logicalTypesVersionV2 = "v2"
)

func newParquetDecodeProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*parquetDecodeProcessor, error) {
	handleLogicalTypes, err := conf.FieldString(pFieldHandleLogicalTypes)
	if err != nil {
		return nil, err
	}

	proc := &parquetDecodeProcessor{
		logger: logger,
	}

	switch handleLogicalTypes {
	case logicalTypesVersionV1:
		proc.visitor.version = 1
	case logicalTypesVersionV2:
		proc.visitor.version = 2
	default:
		return nil, fmt.Errorf("invalid value for field %s: %s", pFieldHandleLogicalTypes, handleLogicalTypes)
	}

	return proc, nil
}

type parquetDecodeProcessor struct {
	logger  *service.Logger
	visitor decodingCoercionVisitor
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

		schema := pRdr.Schema()
		for _, row := range rowBuf[:n] {
			newMsg := msg.Copy()
			row, err = visitWithSchema(&s.visitor, row, schema)
			if err != nil {
				return nil, fmt.Errorf("coercing logical types after decoding: %w", err)
			}
			newMsg.SetStructuredMut(row)
			resBatch = append(resBatch, newMsg)
		}
	}

	return resBatch, nil
}

func (s *parquetDecodeProcessor) Close(ctx context.Context) error {
	return nil
}
