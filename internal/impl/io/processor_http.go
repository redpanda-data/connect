package io

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/public/service"
)

func httpProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Performs an HTTP request using a message batch as the request body, and replaces the original message parts with the body of the response.").
		Description(`
The `+"`rate_limit`"+` field can be used to specify a rate limit [resource](/docs/components/rate_limits/about) to cap the rate of requests across all parallel components service wide.

The URL and header values of this type can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

In order to map or encode the payload to a specific request body, and map the response back into the original payload instead of replacing it entirely, you can use the `+"[`branch` processor](/docs/components/processors/branch)"+`.

## Response Codes

Benthos considers any response code between 200 and 299 inclusive to indicate a successful response, you can add more success status codes with the field `+"`successful_on`"+`.

When a request returns a response code within the `+"`backoff_on`"+` field it will be retried after increasing intervals.

When a request returns a response code within the `+"`drop_on`"+` field it will not be reattempted and is immediately considered a failed request.

## Adding Metadata

If the request returns an error response code this processor sets a metadata field `+"`http_status_code`"+` on the resulting message.

Use the field `+"`extract_headers`"+` to specify rules for which other headers should be copied into the resulting message from the response.

## Error Handling

When all retry attempts for a message are exhausted the processor cancels the attempt. These failed messages will continue through the pipeline unchanged, but can be dropped or placed in a dead letter queue according to your config, you can read about these patterns [here](/docs/configuration/error_handling).`).
		Example(
			"Branched Request",
			`This example uses a `+"[`branch` processor](/docs/components/processors/branch/)"+` to strip the request message into an empty body, grab an HTTP payload, and place the result back into the original message at the path `+"`repo.status`"+`:`,
			`
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
		).
		Field(httpclient.ConfigField("POST", false,
			service.NewBoolField("batch_as_multipart").Description("Send message batches as a single request using [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html).").Advanced().Default(false),
			service.NewBoolField("parallel").Description("When processing batched messages, whether to send messages of the batch in parallel, otherwise they are sent serially.").Default(false)),
		)
}

func init() {
	err := service.RegisterBatchProcessor(
		"http", httpProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			oldMgr := interop.UnwrapManagement(mgr)
			p, err := newHTTPProcFromParsed(conf, oldMgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewV2BatchedToV1Processor("http", p, oldMgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type httpProc struct {
	client      *httpclient.Client
	asMultipart bool
	parallel    bool
	rawURL      string
	log         log.Modular
}

func newHTTPProcFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (processor.V2Batched, error) {
	genericConf, err := conf.FieldAny()
	if err != nil {
		return nil, err
	}

	oldConf, err := httpclient.ConfigFromAny(genericConf)
	if err != nil {
		return nil, err
	}

	asMultipart, err := conf.FieldBool("batch_as_multipart")
	if err != nil {
		return nil, err
	}

	parallel, err := conf.FieldBool("parallel")
	if err != nil {
		return nil, err
	}

	g := &httpProc{
		rawURL:      oldConf.URL,
		log:         mgr.Logger(),
		asMultipart: asMultipart,
		parallel:    parallel,
	}
	if g.client, err = httpclient.NewClientFromOldConfig(oldConf, mgr); err != nil {
		return nil, err
	}
	return g, nil
}

func (h *httpProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	var responseMsg message.Batch

	if h.asMultipart || msg.Len() == 1 {
		// Easy, just do a single request.
		resultMsg, err := h.client.Send(context.Background(), msg)
		if err != nil {
			var code int
			var hErr component.ErrUnexpectedHTTPRes
			if ok := errors.As(err, &hErr); ok {
				code = hErr.Code
			}
			h.log.Errorf("HTTP request to '%v' failed: %v", h.rawURL, err)
			responseMsg = msg.ShallowCopy()
			_ = responseMsg.Iter(func(i int, p *message.Part) error {
				if code > 0 {
					p.MetaSetMut("http_status_code", code)
				}
				p.ErrorSet(err)
				return nil
			})
		} else {
			parts := make([]*message.Part, resultMsg.Len())
			_ = resultMsg.Iter(func(i int, p *message.Part) error {
				if i < msg.Len() {
					parts[i] = msg.Get(i).ShallowCopy()
				} else {
					parts[i] = msg.Get(0).ShallowCopy()
				}
				parts[i].SetBytes(p.AsBytes())
				_ = p.MetaIterMut(func(k string, v any) error {
					parts[i].MetaSetMut(k, v)
					return nil
				})
				return nil
			})
			responseMsg = parts
		}
	} else if !h.parallel {
		responseMsg = message.QuickBatch(nil)

		_ = msg.Iter(func(i int, p *message.Part) error {
			tmpMsg := message.QuickBatch(nil)
			tmpMsg = append(tmpMsg, p)
			result, err := h.client.Send(context.Background(), tmpMsg)
			if err != nil {
				h.log.Errorf("HTTP request to '%v' failed: %v", h.rawURL, err)

				errPart := p.ShallowCopy()
				var hErr component.ErrUnexpectedHTTPRes
				if ok := errors.As(err, &hErr); ok {
					errPart.MetaSetMut("http_status_code", hErr.Code)
				}
				errPart.ErrorSet(err)
				responseMsg = append(responseMsg, errPart)
				return nil
			}

			_ = result.Iter(func(i int, rp *message.Part) error {
				tmpPart := p.ShallowCopy()
				tmpPart.SetBytes(rp.AsBytes())
				_ = rp.MetaIterMut(func(k string, v any) error {
					tmpPart.MetaSetMut(k, v)
					return nil
				})
				responseMsg = append(responseMsg, tmpPart)
				return nil
			})
			return nil
		})
	} else {
		// Hard, need to do parallel requests limited by max parallelism.
		results := make([]*message.Part, msg.Len())
		_ = msg.Iter(func(i int, p *message.Part) error {
			results[i] = p.ShallowCopy()
			return nil
		})
		reqChan, resChan := make(chan int), make(chan error)

		for i := 0; i < msg.Len(); i++ {
			go func() {
				for index := range reqChan {
					tmpMsg := message.Batch{msg.Get(index)}
					result, err := h.client.Send(context.Background(), tmpMsg)
					if err == nil && result.Len() != 1 {
						err = fmt.Errorf("unexpected response size: %v", result.Len())
					}
					if err == nil {
						results[index].SetBytes(result.Get(0).AsBytes())
						_ = result.Get(0).MetaIterMut(func(k string, v any) error {
							results[index].MetaSetMut(k, v)
							return nil
						})
					} else {
						var hErr component.ErrUnexpectedHTTPRes
						if ok := errors.As(err, &hErr); ok {
							results[index].MetaSetMut("http_status_code", hErr.Code)
						}
						results[index].ErrorSet(err)
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
				h.log.Errorf("HTTP parallel request to '%v' failed: %v", h.rawURL, err)
			}
		}

		close(reqChan)
		responseMsg = results
	}

	if responseMsg.Len() < 1 {
		return nil, fmt.Errorf("HTTP response from '%v' was empty", h.rawURL)
	}
	return []message.Batch{responseMsg}, nil
}

func (h *httpProc) Close(ctx context.Context) error {
	return h.client.Close(ctx)
}
