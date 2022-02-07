package input

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	httpdocs "github.com/Jeffail/benthos/v3/internal/http/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	imetadata "github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	httputil "github.com/Jeffail/benthos/v3/lib/util/http"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

//------------------------------------------------------------------------------

func init() {
	corsSpec := httpdocs.ServerCORSFieldSpec()
	corsSpec.Description += " Only valid with a custom `address`."

	Constructors[TypeHTTPServer] = TypeSpec{
		constructor: fromSimpleConstructor(NewHTTPServer),
		Summary: `
Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS, which is enabled when key and cert files are specified.`,
		Description: `
If the ` + "`address`" + ` config field is left blank the [service-wide HTTP server](/docs/components/http/about) will be used.

The field ` + "`rate_limit`" + ` allows you to specify an optional ` + "[`rate_limit` resource](/docs/components/rate_limits/about)" + `, which will be applied to each HTTP request made and each websocket payload received.

When the rate limit is breached HTTP requests will have a 429 response returned with a Retry-After header. Websocket payloads will be dropped and an optional response payload will be sent as per ` + "`ws_rate_limit_message`" + `.

### Responses

It's possible to return a response for each message received using [synchronous responses](/docs/guides/sync_responses). When doing so you can customise headers with the ` + "`sync_response` field `headers`" + `, which can also use [function interpolation](/docs/configuration/interpolation#bloblang-queries) in the value based on the response message contents.

### Endpoints

The following fields specify endpoints that are registered for sending messages, and support path parameters of the form ` + "`/{foo}`" + `, which are added to ingested messages as metadata:

#### ` + "`path` (defaults to `/post`)" + `

This endpoint expects POST requests where the entire request body is consumed as a single message.

If the request contains a multipart ` + "`content-type`" + ` header as per [rfc1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html) then the multiple parts are consumed as a batch of messages, where each body part is a message of the batch.

#### ` + "`ws_path` (defaults to `/post/ws`)" + `

Creates a websocket connection, where payloads received on the socket are passed through the pipeline as a batch of one message.

You may specify an optional ` + "`ws_welcome_message`" + `, which is a static payload to be sent to all clients once a websocket connection is first established.

It's also possible to specify a ` + "`ws_rate_limit_message`" + `, which is a static payload to be sent to clients that have triggered the servers rate limit.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- http_server_user_agent
- http_server_request_path
- http_server_verb
- All headers (only first values are taken)
- All query parameters
- All path parameters
- All cookies
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("address", "An alternative address to host from. If left empty the service wide address is used."),
			docs.FieldCommon("path", "The endpoint path to listen for POST requests."),
			docs.FieldCommon("ws_path", "The endpoint path to create websocket connections from."),
			docs.FieldAdvanced("ws_welcome_message", "An optional message to deliver to fresh websocket connections."),
			docs.FieldAdvanced("ws_rate_limit_message", "An optional message to delivery to websocket connections that are rate limited."),
			docs.FieldCommon("allowed_verbs", "An array of verbs that are allowed for the `path` endpoint.").AtVersion("3.33.0").Array(),
			docs.FieldCommon("timeout", "Timeout for requests. If a consumed messages takes longer than this to be delivered the connection is closed, but the message may still be delivered."),
			docs.FieldCommon("rate_limit", "An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by."),
			docs.FieldAdvanced("cert_file", "Enable TLS by specifying a certificate and key file. Only valid with a custom `address`."),
			docs.FieldAdvanced("key_file", "Enable TLS by specifying a certificate and key file. Only valid with a custom `address`."),
			corsSpec,
			docs.FieldAdvanced("sync_response", "Customise messages returned via [synchronous responses](/docs/guides/sync_responses).").WithChildren(
				docs.FieldCommon(
					"status",
					"Specify the status code to return with synchronous responses. This is a string value, which allows you to customize it based on resulting payloads and their metadata.",
					"200", `${! json("status") }`, `${! meta("status") }`,
				).IsInterpolated(),
				docs.FieldString("headers", "Specify headers to return with synchronous responses.").IsInterpolated().Map().HasDefault(map[string]string{
					"Content-Type": "application/octet-stream",
				}),
				docs.FieldCommon("metadata_headers", "Specify criteria for which metadata values are added to the response as headers.").WithChildren(imetadata.IncludeFilterDocs()...),
			),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// HTTPServerResponseConfig provides config fields for customising the response
// given from successful requests.
type HTTPServerResponseConfig struct {
	Status          string                        `json:"status" yaml:"status"`
	Headers         map[string]string             `json:"headers" yaml:"headers"`
	ExtractMetadata imetadata.IncludeFilterConfig `json:"metadata_headers" yaml:"metadata_headers"`
}

// NewHTTPServerResponseConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerResponseConfig() HTTPServerResponseConfig {
	return HTTPServerResponseConfig{
		Status: "200",
		Headers: map[string]string{
			"Content-Type": "application/octet-stream",
		},
		ExtractMetadata: imetadata.NewIncludeFilterConfig(),
	}
}

// HTTPServerConfig contains configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address            string                   `json:"address" yaml:"address"`
	Path               string                   `json:"path" yaml:"path"`
	WSPath             string                   `json:"ws_path" yaml:"ws_path"`
	WSWelcomeMessage   string                   `json:"ws_welcome_message" yaml:"ws_welcome_message"`
	WSRateLimitMessage string                   `json:"ws_rate_limit_message" yaml:"ws_rate_limit_message"`
	AllowedVerbs       []string                 `json:"allowed_verbs" yaml:"allowed_verbs"`
	Timeout            string                   `json:"timeout" yaml:"timeout"`
	RateLimit          string                   `json:"rate_limit" yaml:"rate_limit"`
	CertFile           string                   `json:"cert_file" yaml:"cert_file"`
	KeyFile            string                   `json:"key_file" yaml:"key_file"`
	CORS               httpdocs.ServerCORS      `json:"cors" yaml:"cors"`
	Response           HTTPServerResponseConfig `json:"sync_response" yaml:"sync_response"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:            "",
		Path:               "/post",
		WSPath:             "/post/ws",
		WSWelcomeMessage:   "",
		WSRateLimitMessage: "",
		AllowedVerbs: []string{
			"POST",
		},
		Timeout:   "5s",
		RateLimit: "",
		CertFile:  "",
		KeyFile:   "",
		CORS:      httpdocs.NewServerCORS(),
		Response:  NewHTTPServerResponseConfig(),
	}
}

//------------------------------------------------------------------------------

// HTTPServer is an input type that registers a range of HTTP endpoints where
// requests can send messages through Benthos. The endpoints are registered on
// the general Benthos HTTP server by default. It is also possible to specify a
// custom address to bind a new server to which the endpoints will be registered
// on instead.
type HTTPServer struct {
	conf  HTTPServerConfig
	stats metrics.Type
	log   log.Modular
	mgr   types.Manager

	mux     *http.ServeMux
	server  *http.Server
	timeout time.Duration

	responseStatus  *field.Expression
	responseHeaders map[string]*field.Expression
	metaFilter      *imetadata.IncludeFilter

	handlerWG    sync.WaitGroup
	transactions chan types.Transaction

	shutSig *shutdown.Signaller

	allowedVerbs map[string]struct{}

	// TODO: V4 Reduce this way down
	mCount         metrics.StatCounter
	mLatency       metrics.StatTimer
	mRateLimited   metrics.StatCounter
	mWSRateLimited metrics.StatCounter
	mRcvd          metrics.StatCounter
	mPartsRcvd     metrics.StatCounter
	mWSCount       metrics.StatCounter
	mTimeout       metrics.StatCounter
	mErr           metrics.StatCounter
	mWSErr         metrics.StatCounter
	mSucc          metrics.StatCounter
	mWSSucc        metrics.StatCounter
	mAsyncErr      metrics.StatCounter
	mAsyncSucc     metrics.StatCounter
}

// NewHTTPServer creates a new HTTPServer input type.
func NewHTTPServer(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var mux *http.ServeMux
	var server *http.Server

	var err error
	if len(conf.HTTPServer.Address) > 0 {
		mux = http.NewServeMux()
		server = &http.Server{Addr: conf.HTTPServer.Address}
		if server.Handler, err = conf.HTTPServer.CORS.WrapHandler(mux); err != nil {
			return nil, fmt.Errorf("bad CORS configuration: %w", err)
		}
	}

	var timeout time.Duration
	if len(conf.HTTPServer.Timeout) > 0 {
		if timeout, err = time.ParseDuration(conf.HTTPServer.Timeout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	verbs := map[string]struct{}{}
	for _, v := range conf.HTTPServer.AllowedVerbs {
		verbs[v] = struct{}{}
	}
	if len(verbs) == 0 {
		return nil, errors.New("must provide at least one allowed verb")
	}

	h := HTTPServer{
		shutSig:         shutdown.NewSignaller(),
		conf:            conf.HTTPServer,
		stats:           stats,
		log:             log,
		mgr:             mgr,
		mux:             mux,
		server:          server,
		timeout:         timeout,
		responseHeaders: map[string]*field.Expression{},
		transactions:    make(chan types.Transaction),

		allowedVerbs: verbs,

		mCount:         stats.GetCounter("count"),
		mLatency:       stats.GetTimer("latency"),
		mRateLimited:   stats.GetCounter("rate_limited"),
		mWSRateLimited: stats.GetCounter("ws.rate_limited"),
		mRcvd:          stats.GetCounter("batch.received"),
		mPartsRcvd:     stats.GetCounter("received"),
		mWSCount:       stats.GetCounter("ws.count"),
		mTimeout:       stats.GetCounter("send.timeout"),
		mErr:           stats.GetCounter("send.error"),
		mWSErr:         stats.GetCounter("ws.send.error"),
		mSucc:          stats.GetCounter("send.success"),
		mWSSucc:        stats.GetCounter("ws.send.success"),
		mAsyncErr:      stats.GetCounter("send.async_error"),
		mAsyncSucc:     stats.GetCounter("send.async_success"),
	}

	if h.responseStatus, err = interop.NewBloblangField(mgr, h.conf.Response.Status); err != nil {
		return nil, fmt.Errorf("failed to parse response status expression: %v", err)
	}
	for k, v := range h.conf.Response.Headers {
		if h.responseHeaders[strings.ToLower(k)], err = interop.NewBloblangField(mgr, v); err != nil {
			return nil, fmt.Errorf("failed to parse response header '%v' expression: %v", k, err)
		}
	}

	if h.metaFilter, err = h.conf.Response.ExtractMetadata.CreateFilter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	postHdlr := httputil.GzipHandler(h.postHandler)
	wsHdlr := httputil.GzipHandler(h.wsHandler)
	if mux != nil {
		if len(h.conf.Path) > 0 {
			mux.HandleFunc(h.conf.Path, postHdlr)
		}
		if len(h.conf.WSPath) > 0 {
			mux.HandleFunc(h.conf.WSPath, wsHdlr)
		}
	} else {
		if len(h.conf.Path) > 0 {
			mgr.RegisterEndpoint(
				h.conf.Path, "Post a message into Benthos.", postHdlr,
			)
		}
		if len(h.conf.WSPath) > 0 {
			mgr.RegisterEndpoint(
				h.conf.WSPath, "Post messages via websocket into Benthos.", wsHdlr,
			)
		}
	}

	if h.conf.RateLimit != "" {
		if err := interop.ProbeRateLimit(context.Background(), h.mgr, h.conf.RateLimit); err != nil {
			return nil, err
		}
	}

	go h.loop()
	return &h, nil
}

//------------------------------------------------------------------------------

func (h *HTTPServer) extractMessageFromRequest(r *http.Request) (*message.Batch, error) {
	msg := message.QuickBatch(nil)

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			var p *multipart.Part
			if p, err = mr.NextPart(); err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			var msgBytes []byte
			if msgBytes, err = io.ReadAll(p); err != nil {
				return nil, err
			}
			msg.Append(message.NewPart(msgBytes))
		}
	} else {
		var msgBytes []byte
		if msgBytes, err = io.ReadAll(r.Body); err != nil {
			return nil, err
		}
		msg.Append(message.NewPart(msgBytes))
	}

	meta := map[string]string{}
	meta["http_server_user_agent"] = r.UserAgent()
	meta["http_server_request_path"] = r.URL.Path
	meta["http_server_verb"] = r.Method
	for k, v := range r.Header {
		if len(v) > 0 {
			meta[k] = v[0]
		}
	}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			meta[k] = v[0]
		}
	}
	for k, v := range mux.Vars(r) {
		meta[k] = v
	}
	for _, c := range r.Cookies() {
		meta[c.Name] = c.Value
	}
	message.SetAllMetadata(msg, meta)

	textMapGeneric := map[string]interface{}{}
	for k, vals := range r.Header {
		for _, v := range vals {
			textMapGeneric[k] = v
		}
	}

	_ = tracing.InitSpansFromParentTextMap("input_http_server_post", textMapGeneric, msg)
	return msg, nil
}

func (h *HTTPServer) postHandler(w http.ResponseWriter, r *http.Request) {
	h.handlerWG.Add(1)
	defer h.handlerWG.Done()
	defer r.Body.Close()

	if _, exists := h.allowedVerbs[r.Method]; !exists {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	if h.conf.RateLimit != "" {
		var tUntil time.Duration
		var err error
		if rerr := interop.AccessRateLimit(r.Context(), h.mgr, h.conf.RateLimit, func(rl types.RateLimit) {
			tUntil, err = rl.Access(r.Context())
		}); rerr != nil {
			http.Error(w, "Server error", http.StatusBadGateway)
			h.log.Warnf("Failed to access rate limit: %v\n", rerr)
			return
		}
		if err != nil {
			http.Error(w, "Server error", http.StatusBadGateway)
			h.log.Warnf("Failed to access rate limit: %v\n", err)
			return
		} else if tUntil > 0 {
			w.Header().Add("Retry-After", strconv.Itoa(int(tUntil.Seconds())))
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			h.mRateLimited.Incr(1)
			return
		}
	}

	msg, err := h.extractMessageFromRequest(r)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		h.log.Warnf("Request read failed: %v\n", err)
		return
	}
	defer tracing.FinishSpans(msg)

	startedAt := time.Now()

	store := roundtrip.NewResultStore()
	roundtrip.AddResultStore(msg, store)

	h.mCount.Incr(1)
	h.mPartsRcvd.Incr(int64(msg.Len()))
	h.mRcvd.Incr(1)
	h.log.Tracef("Consumed %v messages from POST to '%v'.\n", msg.Len(), h.conf.Path)

	resChan := make(chan types.Response, 1)
	select {
	case h.transactions <- types.NewTransaction(msg, resChan):
	case <-time.After(h.timeout):
		h.mTimeout.Incr(1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-r.Context().Done():
		h.mTimeout.Incr(1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.shutSig.CloseAtLeisureChan():
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	select {
	case res, open := <-resChan:
		if !open {
			http.Error(w, "Server closing", http.StatusServiceUnavailable)
			return
		} else if res.Error() != nil {
			h.mErr.Incr(1)
			http.Error(w, res.Error().Error(), http.StatusBadGateway)
			return
		}
		tTaken := time.Since(startedAt).Nanoseconds()
		h.mLatency.Timing(tTaken)
		h.mSucc.Incr(1)
	case <-time.After(h.timeout):
		h.mTimeout.Incr(1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-r.Context().Done():
		h.mTimeout.Incr(1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.shutSig.CloseNowChan():
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	responseMsg := message.QuickBatch(nil)
	for _, resMsg := range store.Get() {
		_ = resMsg.Iter(func(i int, part *message.Part) error {
			responseMsg.Append(part)
			return nil
		})
	}
	if responseMsg.Len() > 0 {
		for k, v := range h.responseHeaders {
			w.Header().Set(k, v.String(0, responseMsg))
		}

		statusCode := 200
		if statusCodeStr := h.responseStatus.String(0, responseMsg); statusCodeStr != "200" {
			if statusCode, err = strconv.Atoi(statusCodeStr); err != nil {
				h.log.Errorf("Failed to parse sync response status code expression: %v\n", err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}

		if plen := responseMsg.Len(); plen == 1 {
			part := responseMsg.Get(0)
			_ = part.MetaIter(func(k, v string) error {
				if h.metaFilter.Match(k) {
					w.Header().Set(k, v)
					return nil
				}
				return nil
			})
			payload := part.Get()
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", http.DetectContentType(payload))
			}
			w.WriteHeader(statusCode)
			w.Write(payload)
		} else if plen > 1 {
			customContentType, customContentTypeExists := h.responseHeaders["content-type"]

			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)

			var merr error
			for i := 0; i < plen && merr == nil; i++ {
				part := responseMsg.Get(i)
				_ = part.MetaIter(func(k, v string) error {
					if h.metaFilter.Match(k) {
						w.Header().Set(k, v)
						return nil
					}
					return nil
				})
				payload := part.Get()

				mimeHeader := textproto.MIMEHeader{}
				if customContentTypeExists {
					mimeHeader.Set("Content-Type", customContentType.String(i, responseMsg))
				} else {
					mimeHeader.Set("Content-Type", http.DetectContentType(payload))
				}

				var partWriter io.Writer
				if partWriter, merr = writer.CreatePart(mimeHeader); merr == nil {
					_, merr = io.Copy(partWriter, bytes.NewReader(payload))
				}
			}

			merr = writer.Close()
			if merr == nil {
				w.Header().Del("Content-Type")
				w.Header().Add("Content-Type", writer.FormDataContentType())
				w.WriteHeader(statusCode)
				buf.WriteTo(w)
			} else {
				h.log.Errorf("Failed to return sync response: %v\n", merr)
				w.WriteHeader(http.StatusBadGateway)
			}
		}
	}
}

func (h *HTTPServer) wsHandler(w http.ResponseWriter, r *http.Request) {
	h.handlerWG.Add(1)
	defer h.handlerWG.Done()

	var err error
	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warnf("Websocket request failed: %v\n", err)
		}
	}()

	upgrader := websocket.Upgrader{}

	var ws *websocket.Conn
	if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
		return
	}
	defer ws.Close()

	resChan := make(chan types.Response, 1)
	throt := throttle.New(throttle.OptCloseChan(h.shutSig.CloseAtLeisureChan()))

	if welMsg := h.conf.WSWelcomeMessage; len(welMsg) > 0 {
		if err = ws.WriteMessage(websocket.BinaryMessage, []byte(welMsg)); err != nil {
			h.log.Errorf("Failed to send welcome message: %v\n", err)
		}
	}

	var msgBytes []byte
	for !h.shutSig.ShouldCloseAtLeisure() {
		if msgBytes == nil {
			if _, msgBytes, err = ws.ReadMessage(); err != nil {
				return
			}
			h.mWSCount.Incr(1)
			h.mCount.Incr(1)
		}

		if h.conf.RateLimit != "" {
			var tUntil time.Duration
			if rerr := interop.AccessRateLimit(r.Context(), h.mgr, h.conf.RateLimit, func(rl types.RateLimit) {
				tUntil, err = rl.Access(r.Context())
			}); rerr != nil {
				h.log.Warnf("Failed to access rate limit: %v\n", rerr)
				err = rerr
			}
			if err != nil || tUntil > 0 {
				if err != nil {
					h.log.Warnf("Failed to access rate limit: %v\n", err)
				}
				if rlMsg := h.conf.WSRateLimitMessage; len(rlMsg) > 0 {
					if err = ws.WriteMessage(websocket.BinaryMessage, []byte(rlMsg)); err != nil {
						h.log.Errorf("Failed to send rate limit message: %v\n", err)
					}
				}
				h.mWSRateLimited.Incr(1)
				continue
			}
		}

		msg := message.QuickBatch([][]byte{msgBytes})
		startedAt := time.Now()

		part := msg.Get(0)
		part.MetaSet("http_server_user_agent", r.UserAgent())
		for k, v := range r.Header {
			if len(v) > 0 {
				part.MetaSet(k, v[0])
			}
		}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				part.MetaSet(k, v[0])
			}
		}
		for k, v := range mux.Vars(r) {
			part.MetaSet(k, v)
		}
		for _, c := range r.Cookies() {
			part.MetaSet(c.Name, c.Value)
		}
		tracing.InitSpans("input_http_server_websocket", msg)

		store := roundtrip.NewResultStore()
		roundtrip.AddResultStore(msg, store)

		select {
		case h.transactions <- types.NewTransaction(msg, resChan):
		case <-h.shutSig.CloseAtLeisureChan():
			return
		}
		select {
		case res, open := <-resChan:
			if !open {
				return
			}
			if res.Error() != nil {
				h.mWSErr.Incr(1)
				h.mErr.Incr(1)
				throt.Retry()
			} else {
				tTaken := time.Since(startedAt).Nanoseconds()
				h.mLatency.Timing(tTaken)
				h.mWSSucc.Incr(1)
				h.mSucc.Incr(1)
				msgBytes = nil
				throt.Reset()
			}
		case <-h.shutSig.CloseNowChan():
			return
		}

		for _, responseMsg := range store.Get() {
			if err := responseMsg.Iter(func(i int, part *message.Part) error {
				return ws.WriteMessage(websocket.TextMessage, part.Get())
			}); err != nil {
				h.log.Errorf("Failed to send sync response over websocket: %v\n", err)
			}
		}

		tracing.FinishSpans(msg)
	}
}

//------------------------------------------------------------------------------

func (h *HTTPServer) loop() {
	mRunning := h.stats.GetGauge("running")

	defer func() {
		if h.server != nil {
			if err := h.server.Shutdown(context.Background()); err != nil {
				h.log.Errorf("Failed to gracefully terminate http_server: %v\n", err)
			}
		} else {
			if len(h.conf.Path) > 0 {
				h.mgr.RegisterEndpoint(h.conf.Path, "Does nothing.", http.NotFound)
			}
			if len(h.conf.WSPath) > 0 {
				h.mgr.RegisterEndpoint(h.conf.WSPath, "Does nothing.", http.NotFound)
			}
		}

		h.handlerWG.Wait()
		mRunning.Decr(1)

		close(h.transactions)
		h.shutSig.ShutdownComplete()
	}()
	mRunning.Incr(1)

	if h.server != nil {
		go func() {
			if len(h.conf.KeyFile) > 0 || len(h.conf.CertFile) > 0 {
				h.log.Infof(
					"Receiving HTTPS messages at: https://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServeTLS(
					h.conf.CertFile, h.conf.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			} else {
				h.log.Infof(
					"Receiving HTTP messages at: http://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			}
		}()
	}

	<-h.shutSig.CloseAtLeisureChan()
}

// TransactionChan returns a transactions channel for consuming messages from
// this input.
func (h *HTTPServer) TransactionChan() <-chan types.Transaction {
	return h.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (h *HTTPServer) Connected() bool {
	return true
}

// CloseAsync shuts down the HTTPServer input and stops processing requests.
func (h *HTTPServer) CloseAsync() {
	h.shutSig.CloseAtLeisure()
}

// WaitForClose blocks until the HTTPServer input has closed down.
func (h *HTTPServer) WaitForClose(timeout time.Duration) error {
	go func() {
		<-time.After(timeout - time.Second)
		h.shutSig.CloseNow()
	}()
	select {
	case <-h.shutSig.HasClosedChan():
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
