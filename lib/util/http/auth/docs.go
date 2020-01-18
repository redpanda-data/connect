package auth

import "github.com/Jeffail/benthos/v3/lib/x/docs"

// FieldSpecs returns a map of field specs for an auth type.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldAdvanced("oauth",
			"Allows you to specify open authentication.",
			map[string]interface{}{
				"enabled":             true,
				"consumer_key":        "foo",
				"consumer_secret":     "bar",
				"access_token":        "baz",
				"access_token_secret": "bev",
				"request_url":         "http://thisisjustanexample.com/dontactuallyusethis",
			},
		),
		docs.FieldAdvanced("basic_auth",
			"Allows you to specify basic authentication.",
			map[string]interface{}{
				"enabled":  true,
				"username": "foo",
				"password": "bar",
			},
		),
	}
}
