package parquet

import (
	"bytes"
	"errors"
	"io"

	"github.com/segmentio/parquet-go"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	parquetParseSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Decodes a [Parquet file](https://parquet.apache.org/docs/) into an array of objects, one for each row within the file.").
		Param(bloblang.NewBoolParam("byte_array_as_string").
			Description("Whether to extract BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY values as strings rather than byte slices in all cases. Values with a logical type of UTF8 will automatically be extracted as strings irrespective of this parameter. Enabling this field makes serialising the data as JSON more intuitive as `[]byte` values are serialised as base64 encoded strings by default.").Default(false)).
		Example("", `root = content().parse_parquet()`).
		Example("", `root = content().parse_parquet(byte_array_as_string: true)`)

	if err := bloblang.RegisterMethodV2(
		"parse_parquet", parquetParseSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			var conf extractConfig
			var err error
			if conf.byteArrayAsStrings, err = args.GetBool("byte_array_as_string"); err != nil {
				return nil, err
			}
			return func(v any) (any, error) {
				b, err := query.IGetBytes(v)
				if err != nil {
					return nil, err
				}

				rdr := bytes.NewReader(b)
				pRdr := parquet.NewReader(rdr)

				rowBuf := make([]parquet.Row, 10)
				var result []any

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
						_, _ = conf.extractPQValueGroup(schema.Fields(), row, mappedData, 0, 0)

						result = append(result, mappedData)
					}
				}

				return result, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
