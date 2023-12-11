package parquet

import (
	"bytes"
	"errors"
	"io"

	"github.com/parquet-go/parquet-go"

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
			Description("Deprecated: This parameter is no longer used.").Default(false)).
		Example("", `root = content().parse_parquet()`)

	if err := bloblang.RegisterMethodV2(
		"parse_parquet", parquetParseSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				b, err := query.IGetBytes(v)
				if err != nil {
					return nil, err
				}

				rdr := bytes.NewReader(b)
				pRdr := parquet.NewGenericReader[any](rdr)

				rowBuf := make([]any, 10)
				var result []any

				for {
					n, err := pRdr.Read(rowBuf)
					if err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					if n == 0 {
						break
					}

					for i := 0; i < n; i++ {
						result = append(result, rowBuf[i])
					}
				}

				return result, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
