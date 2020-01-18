package client

import (
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

// FieldSpecs returns a map of field specs for an HTTP type.
func FieldSpecs() docs.FieldSpecs {
	httpSpecs := docs.FieldSpecs{
		docs.FieldCommon("url", "The URL to connect to.").HasType("string"),
		docs.FieldCommon("verb", "A verb to connect with", "POST", "GET", "DELETE").HasType("string"),
		docs.FieldCommon("headers", "A map of headers to add to the request.", map[string]interface{}{
			"Content-Type": "application/octet-stream",
		}).HasType("object"),
	}
	httpSpecs = append(httpSpecs, auth.FieldSpecs()...)
	httpSpecs = append(httpSpecs, tls.FieldSpec())
	httpSpecs = append(httpSpecs,
		docs.FieldAdvanced("copy_response_headers", "Sets whether to copy the headers from the response to the resulting payload.").HasType("bool"),
		docs.FieldCommon("rate_limit", "An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.").HasType("string"),
		docs.FieldCommon("timeout", "A static timeout to apply to requests.").HasType("string"),
		docs.FieldAdvanced("retry_period", "The base period to wait between failed requests.").HasType("string"),
		docs.FieldAdvanced("max_retry_backoff", "The maximum period to wait between failed requests.").HasType("string"),
		docs.FieldAdvanced("retries", "The maximum number of retry attempts to make.").HasType("number"),
		docs.FieldAdvanced("backoff_on", "A list of status codes whereby retries should be attempted but the period between them should be increased gradually.").HasType("array"),
		docs.FieldAdvanced("drop_on", "A list of status codes whereby the attempt should be considered failed but retries should not be attempted.").HasType("array"),
		docs.FieldAdvanced("successful_on", "A list of status codes whereby the attempt should be considered successful (allows you to configure non-2XX codes).").HasType("array"),
	)

	return httpSpecs
}
