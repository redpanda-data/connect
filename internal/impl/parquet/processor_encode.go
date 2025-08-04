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

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func parquetEncodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Encodes https://parquet.apache.org/docs/[Parquet files^] from a batch of structured messages.").
		Fields(
			parquetSchemaConfig().Optional(),
			service.NewStringField("schema_metadata").
				Description("Optionally specify a metadata field containing a schema definition to use for encoding instead of a statically defined schema. For batches of messages, the first message's schema will be applied to all subsequent messages of the batch.").
				Default(""),
			service.NewStringEnumField("default_compression",
				"uncompressed", "snappy", "gzip", "brotli", "zstd", "lz4raw",
			).
				Description("The default compression type to use for fields.").
				Default("uncompressed"),
			service.NewStringEnumField("default_encoding",
				"DELTA_LENGTH_BYTE_ARRAY", "PLAIN",
			).
				Description("The default encoding type to use for fields. A custom default encoding is only necessary when consuming data with libraries that do not support `DELTA_LENGTH_BYTE_ARRAY` and is therefore best left unset where possible.").
				Default("DELTA_LENGTH_BYTE_ARRAY").
				Advanced().
				Version("4.11.0"),
		).
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
`).
		LintRule(`root = if this.schema.or([]).length() == 0 && this.schema_metadata.or("") == "" { "either a schema or schema_metadata must be specified" }`)
}

func init() {
	service.MustRegisterBatchProcessor(
		"parquet_encode", parquetEncodeProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newParquetEncodeProcessorFromConfig(conf, mgr.Logger())
		})
}

//------------------------------------------------------------------------------

func parquetSchemaConfig() *service.ConfigField {
	return service.NewObjectListField("schema",
		service.NewStringField("name").Description("The name of the column."),
		service.NewStringEnumField("type", "BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "BYTE_ARRAY", "UTF8", "TIMESTAMP", "BSON", "ENUM", "JSON", "UUID").
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
			case "TIMESTAMP":
				// TODO: add field to specify timestamp unit (https://github.com/redpanda-data/connect/issues/3570)
				n = parquet.Timestamp(parquet.Nanosecond)
			case "BSON":
				n = parquet.BSON()
			case "ENUM":
				n = parquet.Enum()
			case "JSON":
				n = parquet.JSON()
			case "UUID":
				n = parquet.UUID()
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
	var schema *parquet.Schema
	if conf.Contains("schema") {
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
		schema = parquet.NewSchema("", node)
	}

	schemaMeta, err := conf.FieldString("schema_metadata")
	if err != nil {
		return nil, err
	}

	if schemaMeta == "" && schema == nil {
		return nil, errors.New("either a schema or schema_metadata must be specified")
	}

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
	return newParquetEncodeProcessor(logger, schema, schemaMeta, compressDefault)
}

type parquetEncodeProcessor struct {
	logger          *service.Logger
	schema          *parquet.Schema
	schemaMeta      string
	compressionType compress.Codec
}

func newParquetEncodeProcessor(logger *service.Logger, schema *parquet.Schema, schemaMeta string, compressionType compress.Codec) (*parquetEncodeProcessor, error) {
	s := &parquetEncodeProcessor{
		logger:          logger,
		schema:          schema,
		schemaMeta:      schemaMeta,
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

func (s *parquetEncodeProcessor) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	schema := s.schema
	if s.schemaMeta != "" {
		metaAny, exists := batch[0].MetaGetMut(s.schemaMeta)
		if !exists {
			return nil, fmt.Errorf("schema_metadata '%v' specified but field was missing from input data", s.schemaMeta)
		}

		var err error
		if schema, err = parquetSchemaFromCommon(metaAny); err != nil {
			return nil, err
		}
	}

	buf := bytes.NewBuffer(nil)
	pWtr := parquet.NewGenericWriter[any](buf, schema, parquet.Compression(s.compressionType))

	batch = batch.Copy()
	rows := make([]any, len(batch))
	for i, m := range batch {
		ms, err := m.AsStructuredMut()
		if err != nil {
			return nil, err
		}

		var isObj bool
		if rows[i], isObj = scrubJSONNumbers(ms).(map[string]any); !isObj {
			return nil, fmt.Errorf("unable to encode message type %T as parquet row", ms)
		}

		rows[i], err = visitWithSchema(encodingCoercionVisitor{}, rows[i], schema)
		if err != nil {
			return nil, fmt.Errorf("coercing logical types: %w", err)
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

func (*parquetEncodeProcessor) Close(context.Context) error {
	return nil
}

func parquetNodeFromCommonField(field schema.Common) (parquet.Node, error) {
	var n parquet.Node

	switch field.Type {
	case schema.Boolean:
		n = parquet.Leaf(parquet.BooleanType)
	case schema.Int32:
		n = parquet.Int(32)
	case schema.Int64:
		n = parquet.Int(64)
	case schema.Float32:
		n = parquet.Leaf(parquet.FloatType)
	case schema.Float64:
		n = parquet.Leaf(parquet.DoubleType)
	case schema.String:
		n = parquet.String()
	case schema.Timestamp:
		// TODO: add field to specify timestamp unit (https://github.com/redpanda-data/connect/issues/3570)
		n = parquet.Timestamp(parquet.Nanosecond)
	case schema.ByteArray:
		n = parquet.Leaf(parquet.ByteArrayType)
	case schema.Array:
		if len(field.Children) != 1 {
			return nil, fmt.Errorf("source schema contains array '%v' that does not define a child type", field.Name)
		}

		var err error
		if n, err = parquetNodeFromCommonField(field.Children[0]); err != nil {
			return nil, err
		}
		n = parquet.Repeated(n)

	case schema.Object:
		if len(field.Children) == 0 {
			return nil, fmt.Errorf("source schema contains object '%v' that contains zero children", field.Name)
		}

		var err error
		if n, err = parquetGroupFromCommonFields(field.Children); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("source schema contains field '%v' of type '%v' that is not supported by this processor", field.Name, field.Type)
	}

	if field.Type != schema.Array && field.Optional {
		n = parquet.Optional(n)
	}

	return n, nil
}

func parquetGroupFromCommonFields(fields []schema.Common) (parquet.Group, error) {
	g := parquet.Group{}

	for _, f := range fields {
		n, err := parquetNodeFromCommonField(f)
		if err != nil {
			return nil, err
		}
		g[f.Name] = n
	}

	return g, nil
}

func parquetSchemaFromCommon(a any) (*parquet.Schema, error) {
	commonSchema, err := schema.ParseFromAny(a)
	if err != nil {
		return nil, err
	}

	if commonSchema.Type != schema.Object {
		return nil, fmt.Errorf("source schema must be an object at the root, got %v", commonSchema.Type)
	}

	if len(commonSchema.Children) == 0 {
		return nil, fmt.Errorf("source schema must have at least one field, got %v", len(commonSchema.Children))
	}

	groupNode, err := parquetGroupFromCommonFields(commonSchema.Children)
	if err != nil {
		return nil, err
	}

	return parquet.NewSchema("", groupNode), nil
}
