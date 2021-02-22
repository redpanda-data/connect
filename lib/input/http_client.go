package input

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

func httpClientSpecs() docs.FieldSpecs {
	codecDocs := codec.ReaderDocs.AtVersion("3.42.0")
	codecDocs.Description = "The way in which the bytes of a continuous stream are converted into messages. It's possible to consume lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter. It's not necessary to add gzip in the codec when the response headers specify it as it will be decompressed automatically."
	codecDocs.Examples = []interface{}{"lines", "delim:\t", "delim:foobar", "csv"}

	streamSpecs := docs.FieldSpecs{
		docs.FieldCommon("enabled", "Enables streaming mode.").HasType("bool"),
		docs.FieldCommon("reconnect", "Sets whether to re-establish the connection once it is lost.").HasType("bool"),
		codecDocs,
		docs.FieldAdvanced("max_buffer", "Must be larger than the largest line of the stream.").HasType("number"),
		docs.FieldDeprecated("multipart"),
		docs.FieldDeprecated("delimiter"),
	}

	specs := append(client.FieldSpecs(),
		docs.FieldCommon("payload", "An optional payload to deliver for each request."),
		docs.FieldAdvanced("drop_empty_bodies", "Whether empty payloads received from the target server should be dropped."),
		docs.FieldCommon(
			"stream", "Allows you to set streaming mode, where requests are kept open and messages are processed line-by-line.",
		).WithChildren(streamSpecs...),
	)
	return specs
}

func init() {
	Constructors[TypeHTTPClient] = TypeSpec{
		constructor: fromSimpleConstructor(NewHTTPClient),
		Summary: `
Connects to a server and continuously performs requests for a single message.`,
		Description: `
The URL and header values of this type can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#bloblang-queries).

### Streaming

If you enable streaming then Benthos will consume the body of the response as a continuous stream of data, breaking messages out following a chosen codec. This allows you to consume APIs that provide long lived streamed data feeds (such as Twitter).`,
		FieldSpecs: httpClientSpecs(),
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// StreamConfig contains fields for specifying consumption behaviour when the
// body of a request is a constant stream of bytes.
type StreamConfig struct {
	Enabled   bool   `json:"enabled" yaml:"enabled"`
	Reconnect bool   `json:"reconnect" yaml:"reconnect"`
	Codec     string `json:"codec" yaml:"codec"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// HTTPClientConfig contains configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	client.Config   `json:",inline" yaml:",inline"`
	Payload         string       `json:"payload" yaml:"payload"`
	DropEmptyBodies bool         `json:"drop_empty_bodies" yaml:"drop_empty_bodies"`
	Stream          StreamConfig `json:"stream" yaml:"stream"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	cConf := client.NewConfig()
	cConf.Verb = "GET"
	cConf.URL = "http://localhost:4195/get"
	return HTTPClientConfig{
		Config:          cConf,
		Payload:         "",
		DropEmptyBodies: true,
		Stream: StreamConfig{
			Enabled:   false,
			Reconnect: true,
			Codec:     "lines",
			Multipart: false,
			MaxBuffer: 1000000,
			Delim:     "",
		},
	}
}

//------------------------------------------------------------------------------

// HTTPClient is an input type that continuously makes HTTP requests and reads
// the response bodies as message payloads.
type HTTPClient struct {
	conf HTTPClientConfig

	client  *client.Type
	payload types.Message

	codecCtor codec.ReaderConstructor

	codecMut sync.Mutex
	codec    codec.Reader
}

// NewHTTPClient creates a new HTTPClient input type.
func NewHTTPClient(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	rdr, err := newHTTPClient(conf.HTTPClient, mgr, log, stats)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(TypeHTTPClient, true, reader.NewAsyncPreserver(rdr), log, stats)
}

func newHTTPClient(conf HTTPClientConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*HTTPClient, error) {
	var codecCtor codec.ReaderConstructor

	if conf.Stream.Enabled {
		// Timeout should be left at zero if we are streaming.
		conf.Timeout = ""
		if len(conf.Stream.Delim) > 0 {
			conf.Stream.Codec = "delim:" + conf.Stream.Delim
		}
		if conf.Stream.Multipart && !strings.HasSuffix(conf.Stream.Codec, "/multipart") {
			conf.Stream.Codec = conf.Stream.Codec + "/multipart"
		}
		codecConf := codec.NewReaderConfig()
		codecConf.MaxScanTokenSize = conf.Stream.MaxBuffer

		var err error
		if codecCtor, err = codec.GetReader(conf.Stream.Codec, codecConf); err != nil {
			return nil, err
		}
	}

	var payload types.Message
	if len(conf.Payload) > 0 {
		payload = message.New([][]byte{[]byte(conf.Payload)})
	}

	client, err := client.New(
		conf.Config,
		client.OptSetLogger(log.NewModule(".client")),
		client.OptSetManager(mgr),
		client.OptSetStats(metrics.Namespaced(stats, "client")),
	)
	if err != nil {
		return nil, err
	}

	return &HTTPClient{
		conf:    conf,
		payload: payload,
		client:  client,

		codecCtor: codecCtor,
	}, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection.
func (h *HTTPClient) ConnectWithContext(ctx context.Context) (err error) {
	if !h.conf.Stream.Enabled {
		return nil
	}

	h.codecMut.Lock()
	defer h.codecMut.Unlock()

	if h.codec != nil {
		return nil
	}

	res, err := h.client.DoWithContext(ctx, h.payload)
	if err != nil {
		if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
			err = types.ErrTimeout
		}
		return err
	}

	if h.codec, err = h.codecCtor("", res.Body, func(ctx context.Context, err error) error {
		return nil
	}); err != nil {
		res.Body.Close()
		return err
	}
	return nil
}

// ReadWithContext a new HTTPClient message.
func (h *HTTPClient) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	if h.conf.Stream.Enabled {
		return h.readStreamed(ctx)
	}
	return h.readNotStreamed(ctx)
}

func (h *HTTPClient) readStreamed(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	h.codecMut.Lock()
	defer h.codecMut.Unlock()

	if h.codec == nil {
		return nil, nil, types.ErrNotConnected
	}

	parts, codecAckFn, err := h.codec.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = types.ErrTimeout
		}
		if err != types.ErrTimeout {
			h.codec.Close(ctx)
			h.codec = nil
		}
		if errors.Is(err, io.EOF) {
			if !h.conf.Stream.Reconnect {
				return nil, nil, types.ErrTypeClosed
			}
			return nil, nil, types.ErrTimeout
		}
		return nil, nil, err
	}

	msg := message.New(nil)
	msg.Append(parts...)

	if msg.Len() == 1 && msg.Get(0).IsEmpty() && h.conf.DropEmptyBodies {
		codecAckFn(ctx, nil)
		return nil, nil, types.ErrTimeout
	}
	if msg.Len() == 0 {
		codecAckFn(ctx, nil)
		return nil, nil, types.ErrTimeout
	}

	return msg, func(rctx context.Context, res types.Response) error {
		return codecAckFn(rctx, res.Error())
	}, nil
}

func (h *HTTPClient) readNotStreamed(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	res, err := h.client.Do(h.payload)
	if err != nil {
		if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
			err = types.ErrTimeout
		}
		return nil, nil, err
	}

	var msg types.Message
	if msg, err = h.client.ParseResponse(res); err != nil {
		return nil, nil, err
	}

	if msg.Len() == 0 {
		return nil, nil, types.ErrTimeout
	}
	if msg.Len() == 1 && msg.Get(0).IsEmpty() && h.conf.DropEmptyBodies {
		return nil, nil, types.ErrTimeout
	}

	return msg, func(context.Context, types.Response) error {
		return nil
	}, nil
}

// CloseAsync shuts down the HTTPClient input and stops processing requests.
func (h *HTTPClient) CloseAsync() {
	h.client.CloseAsync()
	go func() {
		h.codecMut.Lock()
		if h.codec != nil {
			h.codec.Close(context.Background())
			h.codec = nil
		}
		h.codecMut.Unlock()
	}()
}

// WaitForClose blocks until the HTTPClient input has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	return nil
}
