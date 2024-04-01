package io

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/gzip"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/httpserver"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/throttle"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/internal/transaction"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	hsiFieldAddress                 = "address"
	hsiFieldPath                    = "path"
	hsiFieldWSPath                  = "ws_path"
	hsiFieldWSWelcomeMessage        = "ws_welcome_message"
	hsiFieldWSRateLimitMessage      = "ws_rate_limit_message"
	hsiFieldAllowedVerbs            = "allowed_verbs"
	hsiFieldTimeout                 = "timeout"
	hsiFieldRateLimit               = "rate_limit"
	hsiFieldCertFile                = "cert_file"
	hsiFieldKeyFile                 = "key_file"
	hsiFieldCORS                    = "cors"
	hsiFieldCORSEnabled             = "enabled"
	hsiFieldCORSAllowedOrigins      = "allowed_origins"
	hsiFieldResponse                = "sync_response"
	hsiFieldResponseStatus          = "status"
	hsiFieldResponseHeaders         = "headers"
	hsiFieldResponseExtractMetadata = "metadata_headers"
)

type hsiConfig struct {
	Address            string
	Path               string
	WSPath             string
	WSWelcomeMessage   string
	WSRateLimitMessage string
	AllowedVerbs       map[string]struct{}
	Timeout            time.Duration
	RateLimit          string
	CertFile           string
	KeyFile            string
	CORS               httpserver.CORSConfig
	Response           hsiResponseConfig
}

type hsiResponseConfig struct {
	Status          *service.InterpolatedString
	Headers         map[string]*service.InterpolatedString
	ExtractMetadata *service.MetadataFilter
}

func hsiConfigFromParsed(pConf *service.ParsedConfig) (conf hsiConfig, err error) {
	if conf.Address, err = pConf.FieldString(hsiFieldAddress); err != nil {
		return
	}
	if conf.Path, err = pConf.FieldString(hsiFieldPath); err != nil {
		return
	}
	if conf.WSPath, err = pConf.FieldString(hsiFieldWSPath); err != nil {
		return
	}
	if conf.WSWelcomeMessage, err = pConf.FieldString(hsiFieldWSWelcomeMessage); err != nil {
		return
	}
	if conf.WSRateLimitMessage, err = pConf.FieldString(hsiFieldWSRateLimitMessage); err != nil {
		return
	}
	{
		var verbsList []string
		if verbsList, err = pConf.FieldStringList(hsiFieldAllowedVerbs); err != nil {
			return
		}
		if len(verbsList) == 0 {
			err = errors.New("must specify at least one allowed verb")
			return
		}
		conf.AllowedVerbs = map[string]struct{}{}
		for _, v := range verbsList {
			conf.AllowedVerbs[v] = struct{}{}
		}
	}
	if conf.Timeout, err = pConf.FieldDuration(hsiFieldTimeout); err != nil {
		return
	}
	if conf.RateLimit, err = pConf.FieldString(hsiFieldRateLimit); err != nil {
		return
	}
	if conf.CertFile, err = pConf.FieldString(hsiFieldCertFile); err != nil {
		return
	}
	if conf.KeyFile, err = pConf.FieldString(hsiFieldKeyFile); err != nil {
		return
	}
	if conf.CORS, err = corsConfigFromParsed(pConf.Namespace(hsiFieldCORS)); err != nil {
		return
	}
	if conf.Response, err = hsiResponseConfigFromParsed(pConf.Namespace(hsiFieldResponse)); err != nil {
		return
	}
	return
}

func corsConfigFromParsed(pConf *service.ParsedConfig) (conf httpserver.CORSConfig, err error) {
	if conf.Enabled, err = pConf.FieldBool(hsiFieldCORSEnabled); err != nil {
		return
	}
	if conf.AllowedOrigins, err = pConf.FieldStringList(hsiFieldCORSAllowedOrigins); err != nil {
		return
	}
	return
}

func hsiResponseConfigFromParsed(pConf *service.ParsedConfig) (conf hsiResponseConfig, err error) {
	if conf.Status, err = pConf.FieldInterpolatedString(hsiFieldResponseStatus); err != nil {
		return
	}
	if conf.Headers, err = pConf.FieldInterpolatedStringMap(hsiFieldResponseHeaders); err != nil {
		return
	}
	if conf.ExtractMetadata, err = pConf.FieldMetadataFilter(hsiFieldResponseExtractMetadata); err != nil {
		return
	}
	return
}

func hsiSpec() *service.ConfigSpec {
	corsSpec := httpserver.ServerCORSFieldSpec()
	corsSpec.Description += " Only valid with a custom `address`."

	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary(`Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS, which is enabled when key and cert files are specified.`).
		Description(`
If the `+"`address`"+` config field is left blank the [service-wide HTTP server](/docs/components/http/about) will be used.

The field `+"`rate_limit`"+` allows you to specify an optional `+"[`rate_limit` resource](/docs/components/rate_limits/about)"+`, which will be applied to each HTTP request made and each websocket payload received.

When the rate limit is breached HTTP requests will have a 429 response returned with a Retry-After header. Websocket payloads will be dropped and an optional response payload will be sent as per `+"`ws_rate_limit_message`"+`.

### Responses

It's possible to return a response for each message received using [synchronous responses](/docs/guides/sync_responses). When doing so you can customise headers with the `+"`sync_response` field `headers`"+`, which can also use [function interpolation](/docs/configuration/interpolation#bloblang-queries) in the value based on the response message contents.

### Endpoints

The following fields specify endpoints that are registered for sending messages, and support path parameters of the form `+"`/{foo}`"+`, which are added to ingested messages as metadata. A path ending in `+"`/`"+` will match against all extensions of that path:

#### `+"`path` (defaults to `/post`)"+`

This endpoint expects POST requests where the entire request body is consumed as a single message.

If the request contains a multipart `+"`content-type`"+` header as per [rfc1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html) then the multiple parts are consumed as a batch of messages, where each body part is a message of the batch.

#### `+"`ws_path` (defaults to `/post/ws`)"+`

Creates a websocket connection, where payloads received on the socket are passed through the pipeline as a batch of one message.

`+api.EndpointCaveats()+`

You may specify an optional `+"`ws_welcome_message`"+`, which is a static payload to be sent to all clients once a websocket connection is first established.

It's also possible to specify a `+"`ws_rate_limit_message`"+`, which is a static payload to be sent to clients that have triggered the servers rate limit.

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- http_server_user_agent
- http_server_request_path
- http_server_verb
- http_server_remote_ip
- All headers (only first values are taken)
- All query parameters
- All path parameters
- All cookies
`+"```"+`

If HTTPS is enabled, the following fields are added as well:
`+"``` text"+`
- http_server_tls_version
- http_server_tls_subject
- http_server_tls_cipher_suite
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(hsiFieldAddress).
				Description("An alternative address to host from. If left empty the service wide address is used.").
				Default(""),
			service.NewStringField(hsiFieldPath).
				Description("The endpoint path to listen for POST requests.").
				Default("/post"),
			service.NewStringField(hsiFieldWSPath).
				Description("The endpoint path to create websocket connections from.").
				Default("/post/ws"),
			service.NewStringField(hsiFieldWSWelcomeMessage).
				Description("An optional message to deliver to fresh websocket connections.").
				Advanced().
				Default(""),
			service.NewStringField(hsiFieldWSRateLimitMessage).
				Description("An optional message to delivery to websocket connections that are rate limited.").
				Advanced().
				Default(""),
			service.NewStringListField(hsiFieldAllowedVerbs).
				Description("An array of verbs that are allowed for the `path` endpoint.").
				Version("3.33.0").
				Default([]any{"POST"}),
			service.NewDurationField(hsiFieldTimeout).
				Description("Timeout for requests. If a consumed messages takes longer than this to be delivered the connection is closed, but the message may still be delivered.").
				Default("5s"),
			service.NewStringField(hsiFieldRateLimit).
				Description("An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.").
				Default(""),
			service.NewStringField(hsiFieldCertFile).
				Description("Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").
				Advanced().
				Default(""),
			service.NewStringField(hsiFieldKeyFile).
				Description("Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").
				Advanced().
				Default(""),
			service.NewInternalField(corsSpec),
			service.NewObjectField(hsiFieldResponse,
				service.NewInterpolatedStringField(hsiFieldResponseStatus).
					Description("Specify the status code to return with synchronous responses. This is a string value, which allows you to customize it based on resulting payloads and their metadata.").
					Examples(`${! json("status") }`, `${! meta("status") }`).
					Default("200"),
				service.NewInterpolatedStringMapField(hsiFieldResponseHeaders).
					Description("Specify headers to return with synchronous responses.").
					Default(map[string]any{
						"Content-Type": "application/octet-stream",
					}),
				service.NewMetadataFilterField(hsiFieldResponseExtractMetadata).
					Description("Specify criteria for which metadata values are added to the response as headers."),
			).
				Description("Customise messages returned via [synchronous responses](/docs/guides/sync_responses).").
				Advanced(),
		).
		Example(
			"Path Switching",
			"This example shows an `http_server` input that captures all requests and processes them by switching on that path:", `
input:
  http_server:
    path: /
    allowed_verbs: [ GET, POST ]
    sync_response:
      headers:
        Content-Type: application/json

  processors:
    - switch:
      - check: '@http_server_request_path == "/foo"'
        processors:
          - mapping: |
              root.title = "You Got Fooed!"
              root.result = content().string().uppercase()

      - check: '@http_server_request_path == "/bar"'
        processors:
          - mapping: 'root.title = "Bar Is Slow"'
          - sleep: # Simulate a slow endpoint
              duration: 1s
`).
		Example(
			"Mock OAuth 2.0 Server",
			"This example shows an `http_server` input that mocks an OAuth 2.0 Client Credentials flow server at the endpoint `/oauth2_test`:", `
input:
  http_server:
    path: /oauth2_test
    allowed_verbs: [ GET, POST ]
    sync_response:
      headers:
        Content-Type: application/json

  processors:
    - log:
        message: "Received request"
        level: INFO
        fields_mapping: |
          root = @
          root.body = content().string()

    - mapping: |
        root.access_token = "MTQ0NjJkZmQ5OTM2NDE1ZTZjNGZmZjI3"
        root.token_type = "Bearer"
        root.expires_in = 3600

    - sync_response: {}
    - mapping: 'root = deleted()'
`)
}

func init() {
	err := service.RegisterBatchInput(
		"http_server", hsiSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			hsiConf, err := hsiConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}

			// TODO: If we refactor this input to implement ReadBatch then we
			// can return a proper service.BatchInput implementation.

			oldMgr := interop.UnwrapManagement(mgr)
			i, err := newHTTPServerInput(hsiConf, oldMgr)
			if err != nil {
				return nil, err
			}

			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpServerInput struct {
	conf hsiConfig
	log  log.Modular
	mgr  bundle.NewManagement

	mux    *mux.Router
	server *http.Server

	handlerWG    sync.WaitGroup
	transactions chan message.Transaction

	shutSig *shutdown.Signaller

	mPostRcvd metrics.StatCounter
	mWSRcvd   metrics.StatCounter
	mLatency  metrics.StatTimer
}

func newHTTPServerInput(conf hsiConfig, mgr bundle.NewManagement) (input.Streamed, error) {
	var gMux *mux.Router
	var server *http.Server

	var err error
	if conf.Address != "" {
		gMux = mux.NewRouter()
		server = &http.Server{Addr: conf.Address}
		if server.Handler, err = conf.CORS.WrapHandler(gMux); err != nil {
			return nil, fmt.Errorf("bad CORS configuration: %w", err)
		}
	}

	mRcvd := mgr.Metrics().GetCounter("input_received")
	h := httpServerInput{
		shutSig:      shutdown.NewSignaller(),
		conf:         conf,
		log:          mgr.Logger(),
		mgr:          mgr,
		mux:          gMux,
		server:       server,
		transactions: make(chan message.Transaction),

		mLatency:  mgr.Metrics().GetTimer("input_latency_ns"),
		mWSRcvd:   mRcvd,
		mPostRcvd: mRcvd,
	}

	postHdlr := gzipHandler(h.postHandler)
	wsHdlr := gzipHandler(h.wsHandler)
	if gMux != nil {
		if h.conf.Path != "" {
			api.GetMuxRoute(gMux, h.conf.Path).Handler(postHdlr)
		}
		if h.conf.WSPath != "" {
			api.GetMuxRoute(gMux, h.conf.WSPath).Handler(wsHdlr)
		}
	} else {
		if h.conf.Path != "" {
			mgr.RegisterEndpoint(
				h.conf.Path, "Post a message into Benthos.", postHdlr,
			)
		}
		if h.conf.WSPath != "" {
			mgr.RegisterEndpoint(
				h.conf.WSPath, "Post messages via websocket into Benthos.", wsHdlr,
			)
		}
	}

	if h.conf.RateLimit != "" {
		if !h.mgr.ProbeRateLimit(h.conf.RateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", h.conf.RateLimit)
		}
	}

	go h.loop()
	return &h, nil
}

//------------------------------------------------------------------------------

func (h *httpServerInput) extractMessageFromRequest(r *http.Request) (message.Batch, error) {
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
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			var msgBytes []byte
			if msgBytes, err = io.ReadAll(p); err != nil {
				return nil, err
			}
			msg = append(msg, message.NewPart(msgBytes))
		}
	} else {
		var msgBytes []byte
		if msgBytes, err = io.ReadAll(r.Body); err != nil {
			return nil, err
		}
		msg = append(msg, message.NewPart(msgBytes))
	}

	_ = msg.Iter(func(i int, p *message.Part) error {
		p.MetaSetMut("http_server_user_agent", r.UserAgent())
		p.MetaSetMut("http_server_request_path", r.URL.Path)
		p.MetaSetMut("http_server_verb", r.Method)
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			p.MetaSetMut("http_server_remote_ip", host)
		}

		if r.TLS != nil {
			var tlsVersion string
			switch r.TLS.Version {
			case tls.VersionTLS10:
				tlsVersion = "TLSv1.0"
			case tls.VersionTLS11:
				tlsVersion = "TLSv1.1"
			case tls.VersionTLS12:
				tlsVersion = "TLSv1.2"
			case tls.VersionTLS13:
				tlsVersion = "TLSv1.3"
			}
			p.MetaSetMut("http_server_tls_version", tlsVersion)
			if len(r.TLS.VerifiedChains) > 0 && len(r.TLS.VerifiedChains[0]) > 0 {
				p.MetaSetMut("http_server_tls_subject", r.TLS.VerifiedChains[0][0].Subject.String())
			}
			p.MetaSetMut("http_server_tls_cipher_suite", tls.CipherSuiteName(r.TLS.CipherSuite))
		}
		for k, v := range r.Header {
			if len(v) > 0 {
				p.MetaSetMut(k, v[0])
			}
		}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				p.MetaSetMut(k, v[0])
			}
		}
		for k, v := range mux.Vars(r) {
			p.MetaSetMut(k, v)
		}
		for _, c := range r.Cookies() {
			p.MetaSetMut(c.Name, c.Value)
		}
		return nil
	})

	textMapGeneric := map[string]any{}
	for k, vals := range r.Header {
		for _, v := range vals {
			textMapGeneric[k] = v
		}
	}

	_ = tracing.InitSpansFromParentTextMap(h.mgr.Tracer(), "input_http_server_post", textMapGeneric, msg)
	return msg, nil
}

func (h *httpServerInput) postHandler(w http.ResponseWriter, r *http.Request) {
	if h.shutSig.ShouldCloseAtLeisure() {
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	h.handlerWG.Add(1)
	defer h.handlerWG.Done()
	defer r.Body.Close()

	if _, exists := h.conf.AllowedVerbs[r.Method]; !exists {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	if h.conf.RateLimit != "" {
		var tUntil time.Duration
		var err error
		if rerr := h.mgr.AccessRateLimit(r.Context(), h.conf.RateLimit, func(rl ratelimit.V1) {
			tUntil, err = rl.Access(r.Context())
		}); rerr != nil {
			http.Error(w, "Server error", http.StatusBadGateway)
			h.log.Warn("Failed to access rate limit: %v\n", rerr)
			return
		}
		if err != nil {
			http.Error(w, "Server error", http.StatusBadGateway)
			h.log.Warn("Failed to access rate limit: %v\n", err)
			return
		} else if tUntil > 0 {
			w.Header().Add("Retry-After", strconv.Itoa(int(tUntil.Seconds())))
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
	}

	msg, err := h.extractMessageFromRequest(r)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		h.log.Warn("Request read failed: %v\n", err)
		return
	}
	defer tracing.FinishSpans(msg)

	startedAt := time.Now()

	store := transaction.NewResultStore()
	transaction.AddResultStore(msg, store)

	h.mPostRcvd.Incr(int64(msg.Len()))
	h.log.Trace("Consumed %v messages from POST to '%v'.\n", msg.Len(), h.conf.Path)

	resChan := make(chan error, 1)
	select {
	case h.transactions <- message.NewTransaction(msg, resChan):
	case <-time.After(h.conf.Timeout):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-r.Context().Done():
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
		} else if res != nil {
			http.Error(w, res.Error(), http.StatusBadGateway)
			return
		}
		tTaken := time.Since(startedAt).Nanoseconds()
		h.mLatency.Timing(tTaken)
	case <-time.After(h.conf.Timeout):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-r.Context().Done():
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.shutSig.CloseNowChan():
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	var svcBatch service.MessageBatch
	for _, resMsg := range store.Get() {
		for i := 0; i < len(resMsg); i++ {
			svcBatch = append(svcBatch, service.NewInternalMessage(resMsg[i]))
		}
	}
	if len(svcBatch) > 0 {
		for k, v := range h.conf.Response.Headers {
			headerStr, err := svcBatch.TryInterpolatedString(0, v)
			if err != nil {
				h.log.Error("Interpolation of response header %v error: %v", k, err)
				continue
			}
			w.Header().Set(k, headerStr)
		}

		statusCode := 200
		statusCodeStr, err := svcBatch.TryInterpolatedString(0, h.conf.Response.Status)
		if err != nil {
			h.log.Error("Interpolation of response status code error: %v", err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if statusCodeStr != "200" {
			if statusCode, err = strconv.Atoi(statusCodeStr); err != nil {
				h.log.Error("Failed to parse sync response status code expression: %v\n", err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}

		if plen := len(svcBatch); plen == 1 {
			part := svcBatch[0]
			_ = h.conf.Response.ExtractMetadata.Walk(part, func(k, v string) error {
				w.Header().Set(k, v)
				return nil
			})
			payload, err := part.AsBytes()
			if err != nil {
				h.log.Error("Failed to extract message bytes for sync response: %v\n", err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", http.DetectContentType(payload))
			}
			w.WriteHeader(statusCode)
			_, _ = w.Write(payload)
		} else if plen > 1 {
			customContentType, customContentTypeExists := h.conf.Response.Headers["content-type"]

			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)

			var merr error
			for i := 0; i < plen && merr == nil; i++ {
				part := svcBatch[i]
				_ = h.conf.Response.ExtractMetadata.Walk(part, func(k, v string) error {
					w.Header().Set(k, v)
					return nil
				})
				payload, err := part.AsBytes()
				if err != nil {
					h.log.Error("Failed to extract message bytes for sync response: %v\n", err)
					continue
				}

				mimeHeader := textproto.MIMEHeader{}
				if customContentTypeExists {
					contentTypeStr, err := svcBatch.TryInterpolatedString(i, customContentType)
					if err != nil {
						h.log.Error("Interpolation of content-type header error: %v", err)
						mimeHeader.Set("Content-Type", http.DetectContentType(payload))
					} else {
						mimeHeader.Set("Content-Type", contentTypeStr)
					}
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
				_, _ = buf.WriteTo(w)
			} else {
				h.log.Error("Failed to return sync response: %v\n", merr)
				w.WriteHeader(http.StatusBadGateway)
			}
		}
	}
}

func (h *httpServerInput) wsHandler(w http.ResponseWriter, r *http.Request) {
	if h.shutSig.ShouldCloseAtLeisure() {
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	h.handlerWG.Add(1)
	defer h.handlerWG.Done()

	var err error
	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warn("Websocket request failed: %v\n", err)
		}
	}()

	upgrader := websocket.Upgrader{}

	var ws *websocket.Conn
	if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
		return
	}
	defer ws.Close()

	resChan := make(chan error, 1)
	throt := throttle.New(throttle.OptCloseChan(h.shutSig.CloseAtLeisureChan()))

	if welMsg := h.conf.WSWelcomeMessage; welMsg != "" {
		if err = ws.WriteMessage(websocket.BinaryMessage, []byte(welMsg)); err != nil {
			h.log.Error("Failed to send welcome message: %v\n", err)
		}
	}

	var msgBytes []byte
	for !h.shutSig.ShouldCloseAtLeisure() {
		if msgBytes == nil {
			if _, msgBytes, err = ws.ReadMessage(); err != nil {
				return
			}
			h.mWSRcvd.Incr(1)
		}

		if h.conf.RateLimit != "" {
			var tUntil time.Duration
			if rerr := h.mgr.AccessRateLimit(r.Context(), h.conf.RateLimit, func(rl ratelimit.V1) {
				tUntil, err = rl.Access(r.Context())
			}); rerr != nil {
				h.log.Warn("Failed to access rate limit: %v\n", rerr)
				err = rerr
			}
			if err != nil || tUntil > 0 {
				if err != nil {
					h.log.Warn("Failed to access rate limit: %v\n", err)
				}
				if rlMsg := h.conf.WSRateLimitMessage; rlMsg != "" {
					if err = ws.WriteMessage(websocket.BinaryMessage, []byte(rlMsg)); err != nil {
						h.log.Error("Failed to send rate limit message: %v\n", err)
					}
				}
				continue
			}
		}

		msg := message.QuickBatch([][]byte{msgBytes})
		startedAt := time.Now()

		part := msg.Get(0)
		part.MetaSetMut("http_server_user_agent", r.UserAgent())
		for k, v := range r.Header {
			if len(v) > 0 {
				part.MetaSetMut(k, v[0])
			}
		}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				part.MetaSetMut(k, v[0])
			}
		}
		for k, v := range mux.Vars(r) {
			part.MetaSetMut(k, v)
		}
		for _, c := range r.Cookies() {
			part.MetaSetMut(c.Name, c.Value)
		}
		tracing.InitSpans(h.mgr.Tracer(), "input_http_server_websocket", msg)

		store := transaction.NewResultStore()
		transaction.AddResultStore(msg, store)

		select {
		case h.transactions <- message.NewTransaction(msg, resChan):
		case <-h.shutSig.CloseAtLeisureChan():
			return
		}
		select {
		case res, open := <-resChan:
			if !open {
				return
			}
			if res != nil {
				throt.Retry()
			} else {
				tTaken := time.Since(startedAt).Nanoseconds()
				h.mLatency.Timing(tTaken)
				msgBytes = nil
				throt.Reset()
			}
		case <-h.shutSig.CloseNowChan():
			return
		}

		for _, responseMsg := range store.Get() {
			if err := responseMsg.Iter(func(i int, part *message.Part) error {
				return ws.WriteMessage(websocket.TextMessage, part.AsBytes())
			}); err != nil {
				h.log.Error("Failed to send sync response over websocket: %v\n", err)
			}
		}

		tracing.FinishSpans(msg)
	}
}

//------------------------------------------------------------------------------

func (h *httpServerInput) loop() {
	defer func() {
		if h.server != nil {
			if err := h.server.Shutdown(context.Background()); err != nil {
				h.log.Error("Failed to gracefully terminate http_server: %v\n", err)
			}
		} else {
			// We are using the service-wide HTTP server. In order to prevent
			// situations where a slow shutdown results in serving an abundance
			// of 503 responses we wait until either the current requests are
			// handled and shutdown can commence, or we've been instructed to
			// close immediately, which prevents these requests from
			// indefinitely blocking shutdown.
			go func() {
				select {
				case <-h.shutSig.HasClosedChan():
				case <-h.shutSig.CloseNowChan():
				}

				if h.conf.Path != "" {
					h.mgr.RegisterEndpoint(h.conf.Path, "Endpoint disabled.", func(w http.ResponseWriter, r *http.Request) {
						http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
					})
				}
				if h.conf.WSPath != "" {
					h.mgr.RegisterEndpoint(h.conf.WSPath, "Endpoint disabled.", func(w http.ResponseWriter, r *http.Request) {
						http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
					})
				}
			}()
		}

		h.handlerWG.Wait()

		close(h.transactions)
		h.shutSig.ShutdownComplete()
	}()

	if h.server != nil {
		go func() {
			if h.conf.KeyFile != "" || h.conf.CertFile != "" {
				h.log.Info(
					"Receiving HTTPS messages at: https://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServeTLS(
					h.conf.CertFile, h.conf.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Error("Server error: %v\n", err)
				}
			} else {
				h.log.Info(
					"Receiving HTTP messages at: http://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Error("Server error: %v\n", err)
				}
			}
		}()
	}

	<-h.shutSig.CloseAtLeisureChan()
}

// TransactionChan returns a transactions channel for consuming messages from
// this input.
func (h *httpServerInput) TransactionChan() <-chan message.Transaction {
	return h.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (h *httpServerInput) Connected() bool {
	return true
}

func (h *httpServerInput) TriggerStopConsuming() {
	h.shutSig.CloseAtLeisure()
}

func (h *httpServerInput) TriggerCloseNow() {
	h.shutSig.CloseNow()
}

func (h *httpServerInput) WaitForClose(ctx context.Context) error {
	select {
	case <-h.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

//------------------------------------------------------------------------------

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	if w.Header().Get("Content-Type") == "" {
		// If no content type, apply sniffing algorithm to un-gzipped body.
		w.Header().Set("Content-Type", http.DetectContentType(b))
	}
	return w.Writer.Write(b)
}

func gzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzr := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		fn(gzr, r)
	}
}
