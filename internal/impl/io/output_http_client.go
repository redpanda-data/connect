package io

import (
	"context"
	"errors"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
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
			service.NewBoolField("parallel").
				Description("When processing batched messages and `batch_as_multipart` is false, whether to send messages of the batch in parallel, otherwise they are sent serially.").
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
			if mIF, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}

			if b, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}

			bo, err = newHTTPClientOutputFromParsed(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type httpClientWriter struct {
	client *httpclient.Client
	log    *service.Logger

	logURL           string
	propResponse     bool
	batchAsMultipart bool
	parallel         bool
}

func newHTTPClientOutputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*httpClientWriter, error) {
	opts := []httpclient.RequestOpt{}

	logURL, _ := conf.FieldString("url")
	propResponse, err := conf.FieldBool("propagate_response")
	if err != nil {
		return nil, err
	}

	if multiPartObjs, _ := conf.FieldObjectList("multipart"); len(multiPartObjs) > 0 {
		parts := make([]httpclient.MultipartExpressions, len(multiPartObjs))
		for i, p := range multiPartObjs {
			var exprPart httpclient.MultipartExpressions
			if exprPart.ContentDisposition, err = p.FieldInterpolatedString("content_disposition"); err != nil {
				return nil, err
			}
			if exprPart.ContentType, err = p.FieldInterpolatedString("content_type"); err != nil {
				return nil, err
			}
			if exprPart.Body, err = p.FieldInterpolatedString("body"); err != nil {
				return nil, err
			}
			parts[i] = exprPart
		}
		opts = append(opts, httpclient.WithExplicitMultipart(parts))
	}

	oldHTTPConf, err := httpclient.ConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	client, err := httpclient.NewClientFromOldConfig(oldHTTPConf, mgr, opts...)
	if err != nil {
		return nil, err
	}

	batchAsMultipart, err := conf.FieldBool("batch_as_multipart")
	if err != nil {
		return nil, err
	}

	parallel, err := conf.FieldBool("parallel")
	if err != nil {
		return nil, err
	}

	return &httpClientWriter{
		client:           client,
		log:              mgr.Logger(),
		logURL:           logURL,
		propResponse:     propResponse,
		batchAsMultipart: batchAsMultipart,
		parallel:         parallel,
	}, nil
}

func (h *httpClientWriter) Connect(ctx context.Context) error {
	h.log.Infof("Sending messages via HTTP requests to: %s\n", h.logURL)
	return nil
}

func (h *httpClientWriter) WriteBatch(ctx context.Context, msg service.MessageBatch) error {
	if len(msg) > 1 && !h.batchAsMultipart {
		if !h.parallel {
			for _, v := range msg {
				if err := h.WriteBatch(ctx, service.MessageBatch{v}); err != nil {
					return err
				}
			}
		} else {
			// Hard, need to do parallel requests limited by max parallelism.
			return h.handleParallelRequests(msg)
		}
	}

	resultMsg, err := h.client.Send(ctx, msg)
	if err == nil && h.propResponse {
		parts := make(service.MessageBatch, len(resultMsg))
		for i, p := range resultMsg {
			if i < len(msg) {
				parts[i] = msg[i]
			} else {
				parts[i] = msg[0].Copy()
			}

			mBytes, err := p.AsBytes()
			if err != nil {
				return err
			}
			parts[i].SetBytes(mBytes)

			_ = p.MetaWalkMut(func(k string, v any) error {
				parts[i].MetaSetMut(k, v)
				return nil
			})
		}
		if err := parts.AddSyncResponse(); err != nil {
			h.log.Warnf("Unable to propagate response to input: %v", err)
		}
	}
	return err
}

func (h *httpClientWriter) handleParallelRequests(msg service.MessageBatch) error {
	results := make(service.MessageBatch, len(msg))
	for i, p := range msg {
		results[i] = p.Copy()
	}
	reqChan, resChan := make(chan int), make(chan error)

	for i := 0; i < len(msg); i++ {
		go func() {
			for index := range reqChan {
				tmpMsg := service.MessageBatch{msg[index]}
				result, err := h.client.Send(context.Background(), tmpMsg)
				if err == nil && len(result) != 1 {
					err = fmt.Errorf("unexpected response size: %v", len(result))
				}
				if err == nil {
					mBytes, _ := result[0].AsBytes()
					results[index].SetBytes(mBytes)
					_ = result[0].MetaWalkMut(func(k string, v any) error {
						results[index].MetaSetMut(k, v)
						return nil
					})
				} else {
					var hErr component.ErrUnexpectedHTTPRes
					if ok := errors.As(err, &hErr); ok {
						results[index].MetaSetMut("http_status_code", hErr.Code)
					}
					results[index].SetError(err)
				}
				resChan <- err
			}
		}()
	}
	go func() {
		for i := 0; i < len(msg); i++ {
			reqChan <- i
		}
	}()
	for i := 0; i < len(msg); i++ {
		if err := <-resChan; err != nil {
			h.log.Errorf("HTTP parallel request to '%v' failed: %v", h.logURL, err)
		}
	}

	close(reqChan)

	if len(results) < 1 {
		return fmt.Errorf("HTTP response from '%v' was empty", h.logURL)
	}
	return nil
}

func (h *httpClientWriter) Close(ctx context.Context) error {
	return h.client.Close(ctx)
}
