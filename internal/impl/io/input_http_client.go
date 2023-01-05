package io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func httpClientInputSpec() docs.FieldSpec {
	codecDocs := codec.ReaderDocs.AtVersion("3.42.0")
	codecDocs.Description = "The way in which the bytes of a continuous stream are converted into messages. It's possible to consume lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter. It's not necessary to add gzip in the codec when the response headers specify it as it will be decompressed automatically."
	codecDocs.Examples = []any{"lines", "delim:\t", "delim:foobar", "csv"}

	streamSpecs := docs.FieldSpecs{
		docs.FieldBool("enabled", "Enables streaming mode."),
		docs.FieldBool("reconnect", "Sets whether to re-establish the connection once it is lost."),
		codecDocs,
		docs.FieldInt("max_buffer", "Must be larger than the largest line of the stream.").Advanced(),
	}

	return httpclient.OldFieldSpec(false,
		docs.FieldString("payload", "An optional payload to deliver for each request.").IsInterpolated(),
		docs.FieldBool("drop_empty_bodies", "Whether empty payloads received from the target server should be dropped.").Advanced(),
		docs.FieldObject(
			"stream", "Allows you to set streaming mode, where requests are kept open and messages are processed line-by-line.",
		).WithChildren(streamSpecs...),
	)
}

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
		rdr, err := newHTTPClientInput(conf.HTTPClient, mgr)
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("http_client", input.NewAsyncPreserver(rdr), mgr)
	}), docs.ComponentSpec{
		Name:    "http_client",
		Summary: `Connects to a server and continuously performs requests for a single message.`,
		Description: `
The URL and header values of this type can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

### Streaming

If you enable streaming then Benthos will consume the body of the response as a continuous stream of data, breaking messages out following a chosen codec. This allows you to consume APIs that provide long lived streamed data feeds (such as Twitter).

### Pagination

This input supports interpolation functions in the ` + "`url` and `headers`" + ` fields where data from the previous successfully consumed message (if there was one) can be referenced. This can be used in order to support basic levels of pagination. However, in cases where pagination depends on logic it is recommended that you use an ` + "[`http` processor](/docs/components/processors/http) instead, often combined with a [`generate` input](/docs/components/inputs/generate)" + ` in order to schedule the processor.`,
		Config: httpClientInputSpec().ChildDefaultAndTypesFromStruct(input.NewHTTPClientConfig()),
		Categories: []string{
			"Network",
		},
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Basic Pagination",
				Summary: "Interpolation functions within the `url` and `headers` fields can be used to reference the previously consumed message, which allows simple pagination.",
				Config: `
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
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpClientInput struct {
	conf input.HTTPClientConfig

	client       *httpclient.Client
	prevResponse message.Batch

	codecCtor codec.ReaderConstructor

	codecMut sync.Mutex
	codec    codec.Reader
}

func newHTTPClientInput(conf input.HTTPClientConfig, mgr bundle.NewManagement) (*httpClientInput, error) {
	var codecCtor codec.ReaderConstructor

	if conf.Stream.Enabled {
		// Timeout should be left at zero if we are streaming.
		conf.Timeout = ""
		codecConf := codec.NewReaderConfig()
		codecConf.MaxScanTokenSize = conf.Stream.MaxBuffer

		var err error
		if codecCtor, err = codec.GetReader(conf.Stream.Codec, codecConf); err != nil {
			return nil, err
		}
	}

	payloadExpr, err := mgr.BloblEnvironment().NewField(conf.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse payload expression: %w", err)
	}

	client, err := httpclient.NewClientFromOldConfig(conf.OldConfig, mgr, httpclient.WithExplicitBody(payloadExpr))
	if err != nil {
		return nil, err
	}

	return &httpClientInput{
		conf:         conf,
		prevResponse: message.QuickBatch(nil),
		client:       client,

		codecCtor: codecCtor,
	}, nil
}

func (h *httpClientInput) Connect(ctx context.Context) (err error) {
	if !h.conf.Stream.Enabled {
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

	p := message.NewPart(nil)
	for k, values := range res.Header {
		if len(values) > 0 {
			p.MetaSetMut(strings.ToLower(k), values[0])
		}
	}
	h.prevResponse = message.Batch{p}

	if h.codec, err = h.codecCtor("", res.Body, func(ctx context.Context, err error) error {
		return nil
	}); err != nil {
		res.Body.Close()
		return err
	}
	return nil
}

func (h *httpClientInput) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	if h.conf.Stream.Enabled {
		return h.readStreamed(ctx)
	}
	return h.readNotStreamed(ctx)
}

func (h *httpClientInput) readStreamed(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	h.codecMut.Lock()
	defer h.codecMut.Unlock()

	if h.codec == nil {
		return nil, nil, component.ErrNotConnected
	}

	parts, codecAckFn, err := h.codec.Next(ctx)
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
			if !h.conf.Stream.Reconnect {
				return nil, nil, component.ErrTypeClosed
			}
			return nil, nil, component.ErrTimeout
		}
		return nil, nil, err
	}

	msg := message.Batch(parts)
	if msg.Len() == 1 && msg.Get(0).IsEmpty() && h.conf.DropEmptyBodies {
		_ = codecAckFn(ctx, nil)
		return nil, nil, component.ErrTimeout
	}
	if msg.Len() == 0 {
		_ = codecAckFn(ctx, nil)
		return nil, nil, component.ErrTimeout
	}

	meta := map[string]string{}
	_ = h.prevResponse.Get(0).MetaIterStr(func(k, v string) error {
		meta[k] = v
		return nil
	})

	resParts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		part := message.NewPart(p.AsBytes())
		for k, v := range meta {
			part.MetaSetMut(k, v)
		}
		resParts = append(resParts, part)
		return nil
	})
	h.prevResponse = message.Batch(resParts)

	return msg, func(rctx context.Context, res error) error {
		return codecAckFn(rctx, res)
	}, nil
}

func (h *httpClientInput) readNotStreamed(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	msg, err := h.client.Send(ctx, h.prevResponse)
	if err != nil {
		if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
			err = component.ErrTimeout
		}
		return nil, nil, err
	}

	if msg.Len() == 0 {
		return nil, nil, component.ErrTimeout
	}
	if msg.Len() == 1 && msg.Get(0).IsEmpty() && h.conf.DropEmptyBodies {
		return nil, nil, component.ErrTimeout
	}

	h.prevResponse = msg
	return msg.ShallowCopy(), func(context.Context, error) error {
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
