package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPClient] = TypeSpec{
		constructor: NewHTTPClient,
		Summary: `
Sends messages to an HTTP server.`,
		Description: `
When the number of retries expires the output will reject the message, the
behaviour after this will depend on the pipeline but usually this simply means
the send is attempted again until successful whilst applying back pressure.

The URL and header values of this type can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

The body of the HTTP request is the raw contents of the message payload. If the
message has multiple parts (is a batch) the request will be sent according to
[RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). This
behaviour can be overridden by [archiving your batches](/docs/configuration/batching#post-batch-processing).

### Propagating Responses

It's possible to propagate the response from each HTTP request back to the input
source by setting ` + "`propagate_response` to `true`" + `. Only inputs that
support [synchronous responses](/docs/guides/sync_responses) are able to make use of
these propagated responses.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			return sanitiseWithBatch(conf.HTTPClient, conf.HTTPClient.Batching)
		},
		Async:   true,
		Batches: true,
		FieldSpecs: client.FieldSpecs().Add(
			docs.FieldAdvanced("propagate_response", "Whether responses from the server should be [propagated back](/docs/guides/sync_responses) to the input."),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).Add(batch.FieldSpec()),
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

// NewHTTPClient creates a new HTTPClient output type.
func NewHTTPClient(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	h, err := writer.NewHTTPClient(conf.HTTPClient, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	var w Type
	if conf.HTTPClient.MaxInFlight == 1 {
		w, err = NewWriter(TypeHTTPClient, h, log, stats)
	} else {
		w, err = NewAsyncWriter(TypeHTTPClient, conf.HTTPClient.MaxInFlight, h, log, stats)
	}
	if err != nil {
		return w, err
	}
	return newBatcherFromConf(conf.HTTPClient.Batching, w, mgr, log, stats)
}

//------------------------------------------------------------------------------
