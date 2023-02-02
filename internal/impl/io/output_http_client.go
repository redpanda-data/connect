package io

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
	"github.com/benthosdev/benthos/v4/public/service"
)

func httpClientOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Sends messages to an HTTP server.").
		Description(output.Description(true, true, `
When the number of retries expires the output will reject the message, the behaviour after this will depend on the pipeline but usually this simply means the send is attempted again until successful whilst applying back pressure.

The URL and header values of this type can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

The body of the HTTP request is the raw contents of the message payload. If the message has multiple parts (is a batch) the request will be sent according to [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). This behaviour can be disabled by setting the field `+"[`batch_as_multipart`](#batch_as_multipart) to `false`"+`.

### Propagating Responses

It's possible to propagate the response from each HTTP request back to the input source by setting `+"`propagate_response` to `true`"+`. Only inputs that support [synchronous responses](/docs/guides/sync_responses) are able to make use of these propagated responses.`)).
		Field(httpclient.ConfigField("POST", true,
			service.NewBoolField("batch_as_multipart").
				Description("Send message batches as a single request using [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). If disabled messages in batches will be sent as individual requests.").
				Advanced().Default(false),
			service.NewBoolField("propagate_response").
				Description("Whether responses from the server should be [propagated back](/docs/guides/sync_responses) to the input.").
				Advanced().Default(false),
			service.NewIntField("max_in_flight").
				Description("The maximum number of parallel message batches to have in flight at any given time.").
				Default(64),
			service.NewBatchPolicyField("batching"),
			service.NewObjectListField("multipart",
				service.NewInterpolatedStringField("content_type").
					Description("The content type of the individual message part.").
					Example("application/bin").
					Default(""),
				service.NewInterpolatedStringField("content_disposition").
					Description("The content disposition of the individual message part.").
					Example(`form-data; name="bin"; filename='${! @AttachmentName }`).
					Default(""),
				service.NewInterpolatedStringField("body").
					Description("The body of the individual message part.").
					Example(`${! this.data.part1 }`).
					Default(""),
			).Description("EXPERIMENTAL: Create explicit multipart HTTP requests by specifying an array of parts to add to the request, each part specified consists of content headers and a data field that can be populated dynamically. If this field is populated it will override the default request creation behaviour.").
				Advanced().Version("3.63.0").Default([]any{}),
		))
}

func init() {
	err := service.RegisterBatchOutput(
		"http_client", httpClientOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (bo service.BatchOutput, b service.BatchPolicy, mIF int, err error) {
			oldMgr := interop.UnwrapManagement(mgr)

			var maxInFlight int
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			var batchAsMultipart bool
			if batchAsMultipart, err = conf.FieldBool("batch_as_multipart"); err != nil {
				return
			}

			var batchPolAny any
			if batchPolAny, err = conf.FieldAny("batching"); err != nil {
				return
			}
			var batchConf batchconfig.Config
			if batchConf, err = batchconfig.FromAny(batchPolAny); err != nil {
				return
			}

			var wr *httpClientWriter
			if wr, err = newHTTPClientOutputFromParsed(conf, oldMgr); err != nil {
				return
			}

			var o output.Streamed
			if o, err = output.NewAsyncWriter("http_client", maxInFlight, wr, oldMgr); err != nil {
				return
			}
			if !batchAsMultipart {
				o = output.OnlySinglePayloads(o)
			}

			if o, err = batcher.NewFromConfig(batchConf, o, oldMgr); err != nil {
				return
			}
			bo = interop.NewUnwrapInternalOutput(o)
			return
		})
	if err != nil {
		panic(err)
	}
}

type httpClientWriter struct {
	client *httpclient.Client
	log    log.Modular

	logURL       string
	propResponse bool
}

func newHTTPClientOutputFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (*httpClientWriter, error) {
	opts := []httpclient.RequestOpt{}

	logURL, _ := conf.FieldString("url")
	propResponse, err := conf.FieldBool("propagate_response")
	if err != nil {
		return nil, err
	}

	if multiPartObjs, _ := conf.FieldObjectList("multipart"); len(multiPartObjs) > 0 {
		parts := make([]httpclient.MultipartExpressions, len(multiPartObjs))
		for i, p := range multiPartObjs {
			contentDisp, err := p.FieldString("content_disposition")
			if err != nil {
				return nil, err
			}
			contentType, err := p.FieldString("content_type")
			if err != nil {
				return nil, err
			}
			body, err := p.FieldString("body")
			if err != nil {
				return nil, err
			}

			var exprPart httpclient.MultipartExpressions
			if exprPart.ContentDisposition, err = mgr.BloblEnvironment().NewField(contentDisp); err != nil {
				return nil, fmt.Errorf("failed to parse multipart %v field content_disposition: %v", i, err)
			}
			if exprPart.ContentType, err = mgr.BloblEnvironment().NewField(contentType); err != nil {
				return nil, fmt.Errorf("failed to parse multipart %v field content_type: %v", i, err)
			}
			if exprPart.Body, err = mgr.BloblEnvironment().NewField(body); err != nil {
				return nil, fmt.Errorf("failed to parse multipart %v field data: %v", i, err)
			}
			parts[i] = exprPart
		}
		opts = append(opts, httpclient.WithExplicitMultipart(parts))
	}

	genericHTTPConf, err := conf.FieldAny()
	if err != nil {
		return nil, err
	}

	oldHTTPConf, err := httpclient.ConfigFromAny(genericHTTPConf)
	if err != nil {
		return nil, err
	}

	client, err := httpclient.NewClientFromOldConfig(oldHTTPConf, mgr, opts...)
	if err != nil {
		return nil, err
	}

	return &httpClientWriter{
		client:       client,
		log:          mgr.Logger(),
		logURL:       logURL,
		propResponse: propResponse,
	}, nil
}

func (h *httpClientWriter) Connect(ctx context.Context) error {
	h.log.Infof("Sending messages via HTTP requests to: %s\n", h.logURL)
	return nil
}

func (h *httpClientWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	resultMsg, err := h.client.Send(ctx, msg)
	if err == nil && h.propResponse {
		parts := make([]*message.Part, resultMsg.Len())
		_ = resultMsg.Iter(func(i int, p *message.Part) error {
			if i < msg.Len() {
				parts[i] = msg.Get(i)
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
		if err := transaction.SetAsResponse(parts); err != nil {
			h.log.Warnf("Unable to propagate response to input: %v", err)
		}
	}
	return err
}

func (h *httpClientWriter) Close(ctx context.Context) error {
	return h.client.Close(ctx)
}
