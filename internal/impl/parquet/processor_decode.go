package parquet

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/segmentio/parquet-go"

	"github.com/benthosdev/benthos/v4/public/service"
)

func parquetDecodeProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing").
		Summary("Decodes [Parquet files](https://parquet.apache.org/docs/) into a batch of structured messages.").
		Field(service.NewBoolField("byte_array_as_string").
			Description("Whether to extract BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values as strings rather than byte slices in all cases. Values with a logical type of UTF8 will automatically be extracted as strings irrespective of this field. Enabling this field makes serialising the data as JSON more intuitive as `[]byte` values are serialised as base64 encoded strings by default.").
			Default(false)).
		Description(`
This processor uses [https://github.com/segmentio/parquet-go](https://github.com/segmentio/parquet-go), which is itself experimental. Therefore changes could be made into how this processor functions outside of major version releases.

By default any BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY value will be extracted as a byte slice (`+"`[]byte`"+`) unless the logical type is UTF8, in which case they are extracted as a string (`+"`string`"+`).

When a value extracted as a byte slice exists within a document which is later JSON serialized by default it will be base 64 encoded into strings, which is the default for arbitrary data fields. It is possible to convert these binary values to strings (or other data types) using Bloblang transformations such as `+"`root.foo = this.foo.string()` or `root.foo = this.foo.encode(\"hex\")`"+`, etc.

However, in cases where all BYTE_ARRAY values are strings within your data it may be easier to set the config field `+"`byte_array_as_string` to `true`"+` in order to automatically extract all of these values as strings.`).
		Version("4.4.0").
		Example("Reading Parquet Files from AWS S3",
			"In this example we consume files from AWS S3 as they're written by listening onto an SQS queue for upload events. We make sure to use the `all-bytes` codec which means files are read into memory in full, which then allows us to use a `parquet_decode` processor to expand each file into a batch of messages. Finally, we write the data out to local files as newline delimited JSON.",
			`
input:
  aws_s3:
    bucket: TODO
    prefix: foos/
    codec: all-bytes
    sqs:
      url: TODO
  processors:
    - parquet_decode:
        byte_array_as_string: true

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
	var eConf extractConfig
	var err error
	if eConf.byteArrayAsStrings, err = conf.FieldBool("byte_array_as_string"); err != nil {
		return nil, err
	}
	return newParquetDecodeProcessor(logger, &eConf)
}

type parquetDecodeProcessor struct {
	logger *service.Logger
	eConf  *extractConfig
}

func newParquetDecodeProcessor(logger *service.Logger, eConf *extractConfig) (*parquetDecodeProcessor, error) {
	s := &parquetDecodeProcessor{
		logger: logger,
		eConf:  eConf,
	}
	return s, nil
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

	pRdr := parquet.NewGenericReader[any](inFile)

	rowBuf := make([]parquet.Row, 10)
	var resBatch service.MessageBatch

	schema := pRdr.Schema()
	for {
		n, err := pRdr.ReadRows(rowBuf)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			row := rowBuf[i]

			mappedData := map[string]any{}
			_, _ = s.eConf.extractPQValueGroup(schema.Fields(), row, mappedData, 0, 0)

			newMsg := msg.Copy()
			newMsg.SetStructuredMut(mappedData)
			resBatch = append(resBatch, newMsg)
		}
	}

	return resBatch, nil
}

func (s *parquetDecodeProcessor) Close(ctx context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

type extractConfig struct {
	byteArrayAsStrings bool
}

func (e *extractConfig) extractPQValueNotRepeated(field parquet.Field, row []parquet.Value, defLevel, repLevel int) (
	extracted any, // The next value extracted from row
	highestDefLevel int, // The highest definition value seen from the extracted value
	remaining []parquet.Value, // The remaining rows
) {
	if len(field.Fields()) > 0 {
		nested := map[string]any{}
		highestDefLevel, row = e.extractPQValueGroup(field.Fields(), row, nested, defLevel, repLevel)
		return nested, highestDefLevel, row
	}

	value := row[0]
	row = row[1:]

	if value.IsNull() {
		return nil, value.RepetitionLevel(), row
	}

	var v any
	switch value.Kind() {
	case parquet.Boolean:
		v = value.Boolean()
	case parquet.Int32:
		v = value.Int32()
	case parquet.Int64:
		v = value.Int64()
	case parquet.Int96:
		// Parse out as strings, otherwise we can't process these values within
		// Bloblang at all (for now).
		v = value.Int96().String()
	case parquet.Float:
		v = value.Float()
	case parquet.Double:
		v = value.Double()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		logType := field.Type().LogicalType()
		if (logType != nil && logType.UTF8 != nil) || e.byteArrayAsStrings {
			v = string(value.ByteArray())
		} else {
			c := make([]byte, len(value.ByteArray()))
			copy(c, value.ByteArray())
			v = c
		}
	default:
		v = value.String()
	}

	return v, value.DefinitionLevel(), row
}

func (e *extractConfig) extractPQValueMaybeRepeated(field parquet.Field, row []parquet.Value, defLevel, repLevel int) (
	extracted any, // The next value extracted from row
	highestDefLevel int, // The highest definition value seen from the extracted value
	remaining []parquet.Value, // The remaining rows
) {
	if !field.Repeated() {
		return e.extractPQValueNotRepeated(field, row, defLevel, repLevel)
	}

	repLevel++
	var elements []any
	var next any

	// The value is repeated zero or more times, but irrespective of that we
	// always process one value. If the definition level of the returned fields
	// is zero then we have zero elements.
	if next, highestDefLevel, row = e.extractPQValueNotRepeated(field, row, defLevel, repLevel); highestDefLevel == 0 {
		return elements, highestDefLevel, row
	}
	elements = append(elements, next)

	// Collect any subsequent values
	for {
		if len(row) == 0 {
			return elements, highestDefLevel, row
		}
		if row[0].RepetitionLevel() < repLevel {
			return elements, highestDefLevel, row
		}

		var tmpHighestDefLevel int
		if next, tmpHighestDefLevel, row = e.extractPQValueNotRepeated(field, row, defLevel, repLevel); tmpHighestDefLevel > highestDefLevel {
			highestDefLevel = tmpHighestDefLevel
		}
		elements = append(elements, next)
	}
}

// https://www.waitingforcode.com/apache-parquet/nested-data-representation-parquet/read
// https://stackoverflow.com/questions/43568132/dremel-repetition-and-definition-level
// https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet
func (e *extractConfig) extractPQValueGroup(
	fields []parquet.Field,
	row []parquet.Value,
	values map[string]any,
	defLevel, repLevel int,
) (
	highestDefLevel int, // The highest definition value seen from the extracted value
	remaining []parquet.Value, // The remaining rows
) {
	for _, field := range fields {
		if len(row) == 0 {
			return highestDefLevel, row
		}

		var tmpHighestDefLevel int
		if row[0].IsNull() && field.Optional() && row[0].DefinitionLevel() == defLevel {
			if len(field.Fields()) == 0 {
				row = row[1:]
				values[field.Name()] = nil
				continue
			}

			nestedValues := map[string]any{}
			if tmpHighestDefLevel, row = e.extractPQValueGroup(
				field.Fields(), row,
				nestedValues,
				defLevel+1, repLevel,
			); tmpHighestDefLevel > defLevel {
				values[field.Name()] = nestedValues
			} else {
				values[field.Name()] = nil
			}
			if tmpHighestDefLevel > highestDefLevel {
				highestDefLevel = tmpHighestDefLevel
			}
			continue
		}

		if values[field.Name()], tmpHighestDefLevel, row = e.extractPQValueMaybeRepeated(field, row, defLevel+1, repLevel); tmpHighestDefLevel > highestDefLevel {
			highestDefLevel = tmpHighestDefLevel
		}
	}
	return highestDefLevel, row
}
