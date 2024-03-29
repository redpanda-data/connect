package io

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/public/service"
)

func httpClientInputSpec() *service.ConfigSpec {
	streamFields := []*service.ConfigField{
		service.NewBoolField("enabled").Description("Enables streaming mode.").Default(false),
		service.NewBoolField("reconnect").Description("Sets whether to re-establish the connection once it is lost.").Default(true),
	}
	streamFields = append(streamFields, interop.OldReaderCodecFields("lines")...)

	streamField := service.NewObjectField("stream", streamFields...).
		Description("Allows you to set streaming mode, where requests are kept open and messages are processed line-by-line.").
		Optional()

	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Connects to a server and continuously performs requests for a single message.").
		Description(`
The URL and header values of this type can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

### Streaming

If you enable streaming then Benthos will consume the body of the response as a continuous stream of data, breaking messages out following a chosen scanner. This allows you to consume APIs that provide long lived streamed data feeds (such as Twitter).

### Pagination

This input supports interpolation functions in the `+"`url` and `headers`"+` fields where data from the previous successfully consumed message (if there was one) can be referenced. This can be used in order to support basic levels of pagination. However, in cases where pagination depends on logic it is recommended that you use an `+"[`http` processor](/docs/components/processors/http) instead, often combined with a [`generate` input](/docs/components/inputs/generate)"+` in order to schedule the processor.`).
		Example(
			"Basic Pagination",
			"Interpolation functions within the `url` and `headers` fields can be used to reference the previously consumed message, which allows simple pagination.",
			`
input:
  http_client:
    url: >-
      https://api.example.com/search?query=allmyfoos&start_time=${! (
        (timestamp_unix()-300).ts_format("2006-01-02T15:04:05Z","UTC").escape_url_query()
      ) }${! ("&next_token="+this.meta.next_token.not_null()) | "" }
    verb: GET
    rate_limit: foo_searches
    oauth2:
      enabled: true
      token_url: https://api.example.com/oauth2/token
      client_key: "${EXAMPLE_KEY}"
      client_secret: "${EXAMPLE_SECRET}"

rate_limit_resources:
  - label: foo_searches
    local:
      count: 1
      interval: 30s
`,
		).
		Field(httpclient.ConfigField("GET", false,
			service.NewInterpolatedStringField("payload").Description("An optional payload to deliver for each request.").Optional(),
			service.NewBoolField("drop_empty_bodies").Description("Whether empty payloads received from the target server should be dropped.").Default(true).Advanced(),
			streamField,
		)).
		Field(service.NewAutoRetryNacksToggleField())
}

func init() {
	err := service.RegisterBatchInput(
		"http_client", httpClientInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			rdr, err := newHTTPClientInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(conf, rdr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpClientInput struct {
	client       *httpclient.Client
	prevResponse service.MessageBatch

	codecCtor       interop.FallbackReaderCodec
	reconnectStream bool
	dropEmptyBodies bool

	codecMut sync.Mutex
	codec    interop.FallbackReaderStream
}

func newHTTPClientInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (*httpClientInput, error) {
	oldConf, err := httpclient.ConfigFromParsed(conf)
	if err != nil {
		return nil, err
	}

	var codecCtor interop.FallbackReaderCodec

	streamEnabled, err := conf.FieldBool("stream", "enabled")
	if err != nil {
		return nil, err
	}
	if streamEnabled {
		// Timeout should be left at zero if we are streaming.
		oldConf.Timeout = 0
		if codecCtor, err = interop.OldReaderCodecFromParsed(conf.Namespace("stream")); err != nil {
			return nil, err
		}
	}
	reconnectStream, _ := conf.FieldBool("stream", "reconnect")

	var payloadExpr *service.InterpolatedString
	if payloadStr, _ := conf.FieldString("payload"); payloadStr != "" {
		if payloadExpr, err = conf.FieldInterpolatedString("payload"); err != nil {
			return nil, err
		}
	}

	dropEmpty, err := conf.FieldBool("drop_empty_bodies")
	if err != nil {
		return nil, err
	}

	client, err := httpclient.NewClientFromOldConfig(oldConf, mgr, httpclient.WithExplicitBody(payloadExpr))
	if err != nil {
		return nil, err
	}

	return &httpClientInput{
		prevResponse: nil,
		client:       client,

		dropEmptyBodies: dropEmpty,
		reconnectStream: reconnectStream,

		codecCtor: codecCtor,
	}, nil
}

func (h *httpClientInput) Connect(ctx context.Context) (err error) {
	if h.codecCtor == nil {
		return nil
	}

	h.codecMut.Lock()
	defer h.codecMut.Unlock()

	if h.codec != nil {
		return nil
	}

	res, err := h.client.SendToResponse(context.Background(), h.prevResponse)
	if err != nil {
		if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
			err = component.ErrTimeout
		}
		return err
	}

	p := service.NewMessage(nil)
	for k, values := range res.Header {
		if len(values) > 0 {
			p.MetaSetMut(strings.ToLower(k), values[0])
		}
	}
	h.prevResponse = service.MessageBatch{p}

	if h.codec, err = h.codecCtor.Create(res.Body, func(ctx context.Context, err error) error {
		return nil
	}, scanner.SourceDetails{}); err != nil {
		res.Body.Close()
		return err
	}
	return nil
}

func (h *httpClientInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if h.codecCtor != nil {
		return h.readStreamed(ctx)
	}
	return h.readNotStreamed(ctx)
}

func (h *httpClientInput) readStreamed(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	h.codecMut.Lock()
	defer h.codecMut.Unlock()

	if h.codec == nil {
		return nil, nil, service.ErrNotConnected
	}

	parts, codecAckFn, err := h.codec.NextBatch(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			h.codec.Close(ctx)
			h.codec = nil
		}
		if errors.Is(err, io.EOF) {
			if !h.reconnectStream {
				return nil, nil, service.ErrEndOfInput
			}
			return nil, nil, component.ErrTimeout
		}
		return nil, nil, err
	}

	if len(parts) == 1 {
		if mBytes, _ := parts[0].AsBytes(); len(mBytes) == 0 && h.dropEmptyBodies {
			_ = codecAckFn(ctx, nil)
			return nil, nil, component.ErrTimeout
		}
	}
	if len(parts) == 0 {
		_ = codecAckFn(ctx, nil)
		return nil, nil, component.ErrTimeout
	}

	meta := map[string]string{}
	if len(h.prevResponse) > 0 {
		_ = h.prevResponse[0].MetaWalk(func(k, v string) error {
			meta[k] = v
			return nil
		})
	}

	h.prevResponse = make(service.MessageBatch, len(parts))
	for i, v := range parts {
		h.prevResponse[i] = v.Copy()
	}

	return parts, func(rctx context.Context, res error) error {
		return codecAckFn(rctx, res)
	}, nil
}

func (h *httpClientInput) readNotStreamed(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	msg, err := h.client.Send(ctx, h.prevResponse)
	if err != nil {
		if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
			err = component.ErrTimeout
		}
		return nil, nil, err
	}

	if len(msg) == 0 {
		return nil, nil, component.ErrTimeout
	}

	mBytes, _ := msg[0].AsBytes()
	if len(msg) == 1 && len(mBytes) == 0 && h.dropEmptyBodies {
		return nil, nil, component.ErrTimeout
	}

	h.prevResponse = msg
	return msg.Copy(), func(context.Context, error) error {
		return nil
	}, nil
}

func (h *httpClientInput) Close(ctx context.Context) (err error) {
	_ = h.client.Close(ctx)

	h.codecMut.Lock()
	defer h.codecMut.Unlock()

	if h.codec != nil {
		err = h.codec.Close(ctx)
		h.codec = nil
	}
	return
}
