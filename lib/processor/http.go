package processor

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/http"
	ihttpdocs "github.com/Jeffail/benthos/v3/internal/http/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTP] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newHTTPProc(conf.HTTP, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2BatchedToV1Processor("http", p, mgr.Metrics()), nil
		},
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Performs an HTTP request using a message batch as the request body, and replaces
the original message parts with the body of the response.`,
		Description: `
If a processed message batch contains more than one message they will be sent in
a single request as a [multipart message](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html).
Alternatively, message batches can be sent in parallel by setting the field
` + "`parallel` to `true`" + `.

The ` + "`rate_limit`" + ` field can be used to specify a rate limit
[resource](/docs/components/rate_limits/about) to cap the rate of requests
across all parallel components service wide.

The URL and header values of this type can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the ` + "[`branch` processor](/docs/components/processors/branch)" + `.

## Response Codes

Benthos considers any response code between 200 and 299 inclusive to indicate a
successful response, you can add more success status codes with the field
` + "`successful_on`" + `.

When a request returns a response code within the ` + "`backoff_on`" + ` field
it will be retried after increasing intervals.

When a request returns a response code within the ` + "`drop_on`" + ` field it
will not be reattempted and is immediately considered a failed request.

## Adding Metadata

If the request returns an error response code this processor sets a metadata
field ` + "`http_status_code`" + ` on the resulting message.

Use the field ` + "`extract_headers`" + ` to specify rules for which other
headers should be copied into the resulting message from the response.

## Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](/docs/configuration/error_handling).`,
		config: ihttpdocs.ClientFieldSpec(false,
			docs.FieldBool("parallel", "When processing batched messages, whether to send messages of the batch in parallel, otherwise they are sent within a single request.").HasDefault(false)),
		Examples: []docs.AnnotatedExample{
			{
				Title: "Branched Request",
				Summary: `
This example uses a ` + "[`branch` processor](/docs/components/processors/branch/)" + ` to strip the request message into an empty body, grab an HTTP payload, and place the result back into the original message at the path ` + "`repo.status`" + `:`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: 'root = ""'
        processors:
          - http:
              url: https://hub.docker.com/v2/repositories/jeffail/benthos
              verb: GET
        result_map: 'root.repo.status = this'
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// HTTPConfig contains configuration fields for the HTTP processor.
type HTTPConfig struct {
	Parallel      bool `json:"parallel" yaml:"parallel"`
	client.Config `json:",inline" yaml:",inline"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Parallel: false,
		Config:   client.NewConfig(),
	}
}

//------------------------------------------------------------------------------

type httpProc struct {
	client   *http.Client
	parallel bool
	rawURL   string
	log      log.Modular
}

func newHTTPProc(conf HTTPConfig, mgr interop.Manager) (processor.V2Batched, error) {
	g := &httpProc{
		rawURL:   conf.URL,
		log:      mgr.Logger(),
		parallel: conf.Parallel,
	}
	var err error
	if g.client, err = http.NewClient(
		conf.Config,
		http.OptSetLogger(mgr.Logger()),
		http.OptSetStats(mgr.Metrics()),
		http.OptSetManager(mgr),
	); err != nil {
		return nil, err
	}
	return g, nil
}

func (h *httpProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg *message.Batch) ([]*message.Batch, error) {
	var responseMsg *message.Batch

	if !h.parallel || msg.Len() == 1 {
		// Easy, just do a single request.
		resultMsg, err := h.client.Send(context.Background(), msg, msg)
		if err != nil {
			var codeStr string
			var hErr component.ErrUnexpectedHTTPRes
			if ok := errors.As(err, &hErr); ok {
				codeStr = strconv.Itoa(hErr.Code)
			}
			h.log.Errorf("HTTP request failed: %v\n", err)
			responseMsg = msg.Copy()
			_ = responseMsg.Iter(func(i int, p *message.Part) error {
				if len(codeStr) > 0 {
					p.MetaSet("http_status_code", codeStr)
				}
				FlagErr(p, err)
				return nil
			})
		} else {
			parts := make([]*message.Part, resultMsg.Len())
			_ = resultMsg.Iter(func(i int, p *message.Part) error {
				if i < msg.Len() {
					parts[i] = msg.Get(i).Copy()
				} else {
					parts[i] = msg.Get(0).Copy()
				}
				parts[i].Set(p.Get())
				_ = p.MetaIter(func(k, v string) error {
					parts[i].MetaSet(k, v)
					return nil
				})
				return nil
			})
			responseMsg = message.QuickBatch(nil)
			responseMsg.Append(parts...)
		}
	} else {
		// Hard, need to do parallel requests limited by max parallelism.
		results := make([]*message.Part, msg.Len())
		_ = msg.Iter(func(i int, p *message.Part) error {
			results[i] = p.Copy()
			return nil
		})
		reqChan, resChan := make(chan int), make(chan error)

		for i := 0; i < msg.Len(); i++ {
			go func() {
				for index := range reqChan {
					tmpMsg := message.QuickBatch(nil)
					tmpMsg.Append(msg.Get(index))
					result, err := h.client.Send(context.Background(), tmpMsg, tmpMsg)
					if err == nil && result.Len() != 1 {
						err = fmt.Errorf("unexpected response size: %v", result.Len())
					}
					if err == nil {
						results[index].Set(result.Get(0).Get())
						_ = result.Get(0).MetaIter(func(k, v string) error {
							results[index].MetaSet(k, v)
							return nil
						})
					} else {
						var hErr component.ErrUnexpectedHTTPRes
						if ok := errors.As(err, &hErr); ok {
							results[index].MetaSet("http_status_code", strconv.Itoa(hErr.Code))
						}
						FlagErr(results[index], err)
					}
					resChan <- err
				}
			}()
		}
		go func() {
			for i := 0; i < msg.Len(); i++ {
				reqChan <- i
			}
		}()
		for i := 0; i < msg.Len(); i++ {
			if err := <-resChan; err != nil {
				h.log.Errorf("HTTP parallel request to '%v' failed: %v\n", h.rawURL, err)
			}
		}

		close(reqChan)
		responseMsg = message.QuickBatch(nil)
		responseMsg.Append(results...)
	}

	if responseMsg.Len() < 1 {
		return nil, fmt.Errorf("HTTP response from '%v' was empty", h.rawURL)
	}
	return []*message.Batch{responseMsg}, nil
}

func (h *httpProc) Close(ctx context.Context) error {
	return h.client.Close(ctx)
}
