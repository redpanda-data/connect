package pure

import (
	"fmt"
	"net/url"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("parse_url_query",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryParsing).
			Description(`Attempts to parse a url-encoded query string and returns a structured result.`),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.StringMethod(func(data string) (interface{}, error) {
				values, err := url.ParseQuery(data)
				if err != nil {
					return nil, fmt.Errorf("failed to parse value as url-encoded data: %w", err)
				}
				return urlValuesToMap(values), nil
			}), nil
		}); err != nil {
		panic(err)
	}
}

func urlValuesToMap(values url.Values) map[string]interface{} {
	root := make(map[string]interface{})

	for k, v := range values {
		if len(v) == 1 {
			root[k] = v[0]
		} else {
			root[k] = v
		}
	}

	return root
}
