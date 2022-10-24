package io

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/http"
	ihttpdocs "github.com/benthosdev/benthos/v4/internal/http/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newHTTPClientOutput), docs.ComponentSpec{
		Name:    "http_client",
		Summary: `Sends messages to an HTTP server.`,
		Description: output.Description(true, true, `
When the number of retries expires the output will reject the message, the behaviour after this will depend on the pipeline but usually this simply means the send is attempted again until successful whilst applying back pressure.

The URL and header values of this type can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

The body of the HTTP request is the raw contents of the message payload. If the message has multiple parts (is a batch) the request will be sent according to [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). This behaviour can be disabled by setting the field `+"[`batch_as_multipart`](#batch_as_multipart) to `false`"+`.

### Propagating Responses

It's possible to propagate the response from each HTTP request back to the input source by setting `+"`propagate_response` to `true`"+`. Only inputs that support [synchronous responses](/docs/guides/sync_responses) are able to make use of these propagated responses.`),
		Config: ihttpdocs.ClientFieldSpec(true,
			docs.FieldBool("batch_as_multipart", "Send message batches as a single request using [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). If disabled messages in batches will be sent as individual requests.").Advanced(),
			docs.FieldBool("propagate_response", "Whether responses from the server should be [propagated back](/docs/guides/sync_responses) to the input.").Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
			docs.FieldObject(
				"multipart", "EXPERIMENTAL: Create explicit multipart HTTP requests by specifying an array of parts to add to the request, each part specified consists of content headers and a data field that can be populated dynamically. If this field is populated it will override the default request creation behaviour.",
			).Array().Advanced().HasDefault([]interface{}{}).WithChildren(
				docs.FieldInterpolatedString("content_type", "The content type of the individual message part.", "application/bin").HasDefault(""),
				docs.FieldInterpolatedString("content_disposition", "The content disposition of the individual message part.", `form-data; name="bin"; filename='${! meta("AttachmentName") }`).HasDefault(""),
				docs.FieldInterpolatedString("body", "The body of the individual message part.", `${! json("data.part1") }`).HasDefault(""),
			).AtVersion("3.63.0"),
		).ChildDefaultAndTypesFromStruct(output.NewHTTPClientConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newHTTPClientOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	h, err := newHTTPClientWriter(conf.HTTPClient, mgr)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("http_client", conf.HTTPClient.MaxInFlight, h, mgr)
	if err != nil {
		return w, err
	}
	if !conf.HTTPClient.BatchAsMultipart {
		w = output.OnlySinglePayloads(w)
	}
	return batcher.NewFromConfig(conf.HTTPClient.Batching, w, mgr)
}

type httpClientWriter struct {
	client *http.Client

	log log.Modular

	conf output.HTTPClientConfig
}

func newHTTPClientWriter(conf output.HTTPClientConfig, mgr bundle.NewManagement) (*httpClientWriter, error) {
	h := httpClientWriter{
		log:  mgr.Logger(),
		conf: conf,
	}

	opts := []func(*http.Client){
		http.OptSetLogger(h.log),
		http.OptSetManager(mgr),
		http.OptSetStats(mgr.Metrics()),
	}

	if len(conf.Multipart) > 0 {
		parts := make([]http.MultipartExpressions, len(conf.Multipart))
		for i, p := range conf.Multipart {
			var exprPart http.MultipartExpressions
			var err error
			if exprPart.ContentDisposition, err = mgr.BloblEnvironment().NewField(p.ContentDisposition); err != nil {
				return nil, fmt.Errorf("failed to parse multipart %v field content_disposition: %v", i, err)
			}
			if exprPart.ContentType, err = mgr.BloblEnvironment().NewField(p.ContentType); err != nil {
				return nil, fmt.Errorf("failed to parse multipart %v field content_type: %v", i, err)
			}
			if exprPart.Body, err = mgr.BloblEnvironment().NewField(p.Body); err != nil {
				return nil, fmt.Errorf("failed to parse multipart %v field data: %v", i, err)
			}
			parts[i] = exprPart
		}
		opts = append(opts, http.OptSetMultiPart(parts))
	}

	var err error
	if h.client, err = http.NewClient(conf.Config, opts...); err != nil {
		return nil, err
	}
	return &h, nil
}

func (h *httpClientWriter) Connect(ctx context.Context) error {
	h.log.Infof("Sending messages via HTTP requests to: %s\n", h.conf.URL)
	return nil
}

func (h *httpClientWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	resultMsg, err := h.client.Send(ctx, msg, msg)
	if err == nil && h.conf.PropagateResponse {
		parts := make([]*message.Part, resultMsg.Len())
		_ = resultMsg.Iter(func(i int, p *message.Part) error {
			if i < msg.Len() {
				parts[i] = msg.Get(i)
			} else {
				parts[i] = msg.Get(0).ShallowCopy()
			}
			parts[i].SetBytes(p.AsBytes())

			_ = p.MetaIter(func(k, v string) error {
				parts[i].MetaSet(k, v)
				return nil
			})

			return nil
		})
		if err := transaction.SetAsResponse(parts); err != nil {
			h.log.Warnf("Unable to propagate response to input: %v", err)
		}
	}
	return err
}

func (h *httpClientWriter) Close(ctx context.Context) error {
	return h.client.Close(ctx)
}
