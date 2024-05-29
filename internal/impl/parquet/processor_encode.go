package parquet

import (
	"bytes"
	"context"
	"fmt"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func parquetEncodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Encodes https://parquet.apache.org/docs/[Parquet files^] from a batch of structured messages.").
		Field(parquetSchemaConfig()).
		Field(service.NewStringEnumField("default_compression",
			"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw",
		).
			Description("The default compression type to use for fields.").
			Default("uncompressed")).
		Field(service.NewStringEnumField("default_encoding",
			"DELTA_LENGTH_BYTE_ARRAY", "PLAIN",
		).
			Description("The default encoding type to use for fields. A custom default encoding is only necessary when consuming data with libraries that do not support `DELTA_LENGTH_BYTE_ARRAY` and is therefore best left unset where possible.").
			Default("DELTA_LENGTH_BYTE_ARRAY").
			Advanced().
			Version("4.11.0")).
		Description(`
This processor uses https://github.com/parquet-go/parquet-go[https://github.com/parquet-go/parquet-go^], which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.
`).
		Version("4.4.0").
		// TODO: Add an example that demonstrates error handling
		Example("Writing Parquet Files to AWS S3",
			"In this example we use the batching mechanism of an `aws_s3` output to collect a batch of messages in memory, which then converts it to a parquet file and uploads it.",
			`
output:
  aws_s3:
    bucket: TODO
    path: 'stuff/${! timestamp_unix() }-${! uuid_v4() }.parquet'
    batching:
      count: 1000
      period: 10s
      processors:
        - parquet_encode:
            schema:
              - name: id
                type: INT64
              - name: weight
                type: DOUBLE
              - name: content
                type: BYTE_ARRAY
            default_compression: zstd
`)
}

func init() {
	err := service.RegisterBatchProcessor(
		"parquet_encode", parquetEncodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newParquetEncodeProcessorFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func parquetSchemaConfig() *service.ConfigField {
	return service.NewObjectListField("schema",
		service.NewStringField("name").Description("The name of the column."),
		service.NewStringEnumField("type", "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "BYTE_ARRAY", "UTF8").
			Description("The type of the column, only applicable for leaf columns with no child fields. Some logical types can be specified here such as UTF8.").Optional(),
		service.NewBoolField("repeated").Description("Whether the field is repeated.").Default(false),
		service.NewBoolField("optional").Description("Whether the field is optional.").Default(false),
		service.NewAnyListField("fields").Description("A list of child fields.").Optional().Example([]any{
			map[string]any{
				"name": "foo",
				"type": "INT64",
			},
			map[string]any{
				"name": "bar",
				"type": "BYTE_ARRAY",
			},
		}),
	).Description("Parquet schema.")
}

type encodingFn func(n parquet.Node) parquet.Node

var defaultEncodingFn encodingFn = func(n parquet.Node) parquet.Node {
	return n
}

var plainEncodingFn encodingFn = func(n parquet.Node) parquet.Node {
	return parquet.Encoded(n, &parquet.Plain)
}

func parquetGroupFromConfig(columnConfs []*service.ParsedConfig, encodingFn encodingFn) (parquet.Group, error) {
	groupNode := parquet.Group{}

	for _, colConf := range columnConfs {
		var n parquet.Node

		name, err := colConf.FieldString("name")
		if err != nil {
			return nil, err
		}

		if childColumns, _ := colConf.FieldAnyList("fields"); len(childColumns) > 0 {
			if n, err = parquetGroupFromConfig(childColumns, encodingFn); err != nil {
				return nil, err
			}
		} else {
			typeStr, err := colConf.FieldString("type")
			if err != nil {
				return nil, err
			}
			switch typeStr {
			case "BOOLEAN":
				n = parquet.Leaf(parquet.BooleanType)
			case "INT32":
				n = parquet.Int(32)
			case "INT64":
				n = parquet.Int(64)
			case "FLOAT":
				n = parquet.Leaf(parquet.FloatType)
			case "DOUBLE":
				n = parquet.Leaf(parquet.DoubleType)
			case "BYTE_ARRAY":
				n = parquet.Leaf(parquet.ByteArrayType)
			case "UTF8":
				n = parquet.String()
			default:
				return nil, fmt.Errorf("field %v type of '%v' not recognised", name, typeStr)
			}
			n = encodingFn(n)
		}

		repeated, _ := colConf.FieldBool("repeated")
		if repeated {
			n = parquet.Repeated(n)
		}

		optional, _ := colConf.FieldBool("optional")
		if optional {
			if repeated {
				return nil, fmt.Errorf("column %v cannot be both repeated and optional", name)
			}
			n = parquet.Optional(n)
		}

		groupNode[name] = n
	}

	return groupNode, nil
}

//------------------------------------------------------------------------------

func newParquetEncodeProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*parquetEncodeProcessor, error) {
	schemaConfs, err := conf.FieldObjectList("schema")
	if err != nil {
		return nil, err
	}

	customEncoding, err := conf.FieldString("default_encoding")
	if err != nil {
		return nil, err
	}
	var encoding encodingFn
	switch customEncoding {
	case "PLAIN":
		encoding = plainEncodingFn
	default:
		encoding = defaultEncodingFn
	}

	node, err := parquetGroupFromConfig(schemaConfs, encoding)
	if err != nil {
		return nil, err
	}

	schema := parquet.NewSchema("", node)
	compressStr, err := conf.FieldString("default_compression")
	if err != nil {
		return nil, err
	}

	var compressDefault compress.Codec
	switch compressStr {
	case "uncompressed":
		compressDefault = &parquet.Uncompressed
	case "snappy":
		compressDefault = &parquet.Snappy
	case "gzip":
		compressDefault = &parquet.Gzip
	case "brotli":
		compressDefault = &parquet.Brotli
	case "zstd":
		compressDefault = &parquet.Zstd
	case "lz4raw":
		compressDefault = &parquet.Lz4Raw
	default:
		return nil, fmt.Errorf("default_compression type %v not recognised", compressStr)
	}
	return newParquetEncodeProcessor(logger, schema, compressDefault)
}

type parquetEncodeProcessor struct {
	logger          *service.Logger
	schema          *parquet.Schema
	compressionType compress.Codec
}

func newParquetEncodeProcessor(logger *service.Logger, schema *parquet.Schema, compressionType compress.Codec) (*parquetEncodeProcessor, error) {
	s := &parquetEncodeProcessor{
		logger:          logger,
		schema:          schema,
		compressionType: compressionType,
	}
	return s, nil
}

func writeWithoutPanic(pWtr *parquet.GenericWriter[any], rows []any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	_, err = pWtr.Write(rows)
	return
}

func closeWithoutPanic(pWtr *parquet.GenericWriter[any]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("encoding panic: %v", r)
		}
	}()

	err = pWtr.Close()
	return
}

func (s *parquetEncodeProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	buf := bytes.NewBuffer(nil)
	pWtr := parquet.NewGenericWriter[any](buf, s.schema, parquet.Compression(s.compressionType))

	rows := make([]any, len(batch))
	for i, m := range batch {
		ms, err := m.AsStructured()
		if err != nil {
			return nil, err
		}

		var isObj bool
		if rows[i], isObj = scrubJSONNumbers(ms).(map[string]any); !isObj {
			return nil, fmt.Errorf("unable to encode message type %T as parquet row", ms)
		}
	}

	if err := writeWithoutPanic(pWtr, rows); err != nil {
		return nil, err
	}
	if err := closeWithoutPanic(pWtr); err != nil {
		return nil, err
	}

	outMsg := batch[0]
	outMsg.SetBytes(buf.Bytes())
	return []service.MessageBatch{{outMsg}}, nil
}

func (s *parquetEncodeProcessor) Close(ctx context.Context) error {
	return nil
}
