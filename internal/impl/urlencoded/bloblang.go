package urlencoded

import (
	"fmt"
	"net/url"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("parse_url_encoded",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryParsing).
			Description(`Attempts to parse a string as url-encoded data and returns a structured result.`),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.BytesMethod(func(data []byte) (interface{}, error) {
				values, err := url.ParseQuery(string(data))

				if err != nil {
					return nil, fmt.Errorf("failed to parse value as url-encoded data: %w", err)
				}

				return toMap(values), nil
			}), nil
		}); err != nil {
		panic(err)
	}
}
