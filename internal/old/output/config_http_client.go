package output

import (
	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/http/docs"
)

// HTTPClientMultipartExpression represents dynamic expressions that define a
// multipart message part in an HTTP request. Specifying one or more of these
// can be used as a way of creating HTTP requests that overrides the default
// behaviour.
type HTTPClientMultipartExpression struct {
	ContentDisposition string `json:"content_disposition" yaml:"content_disposition"`
	ContentType        string `json:"content_type" yaml:"content_type"`
	Body               string `json:"body" yaml:"body"`
}

// HTTPClientConfig contains configuration fields for the HTTPClient output
// type.
type HTTPClientConfig struct {
	docs.Config       `json:",inline" yaml:",inline"`
	BatchAsMultipart  bool                            `json:"batch_as_multipart" yaml:"batch_as_multipart"`
	MaxInFlight       int                             `json:"max_in_flight" yaml:"max_in_flight"`
	PropagateResponse bool                            `json:"propagate_response" yaml:"propagate_response"`
	Batching          policy.Config                   `json:"batching" yaml:"batching"`
	Multipart         []HTTPClientMultipartExpression `json:"multipart" yaml:"multipart"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		Config:            docs.NewConfig(),
		BatchAsMultipart:  false,
		MaxInFlight:       64,
		PropagateResponse: false,
		Batching:          policy.NewConfig(),
	}
}
