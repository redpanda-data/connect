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

	"github.com/klauspost/compress/gzip"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpserver"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	imetadata "github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/old/util/throttle"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

func init() {
	corsSpec := httpserver.ServerCORSFieldSpec()
	corsSpec.Description += " Only valid with a custom `address`."

	err := bundle.AllInputs.Add(processors.WrapConstructor(newHTTPServerInput), docs.ComponentSpec{
		Name:    "http_server",
		Summary: `Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS, which is enabled when key and cert files are specified.`,
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
- http_server_remote_ip
- All headers (only first values are taken)
- All query parameters
- All path parameters
- All cookies
` + "```" + `
If HTTPS is enabled, the following fields are added as well:
` + "``` text" + `
- http_server_tls_version
- http_server_tls_subject
- http_server_tls_cipher_suite
` + "```" + `
You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("address", "An alternative address to host from. If left empty the service wide address is used."),
			docs.FieldString("path", "The endpoint path to listen for POST requests."),
			docs.FieldString("ws_path", "The endpoint path to create websocket connections from."),
			docs.FieldString("ws_welcome_message", "An optional message to deliver to fresh websocket connections.").Advanced(),
			docs.FieldString("ws_rate_limit_message", "An optional message to delivery to websocket connections that are rate limited.").Advanced(),
			docs.FieldString("allowed_verbs", "An array of verbs that are allowed for the `path` endpoint.").AtVersion("3.33.0").Array(),
			docs.FieldString("timeout", "Timeout for requests. If a consumed messages takes longer than this to be delivered the connection is closed, but the message may still be delivered."),
			docs.FieldString("rate_limit", "An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by."),
			docs.FieldString("cert_file", "Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").Advanced(),
			docs.FieldString("key_file", "Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").Advanced(),
			corsSpec,
			docs.FieldObject("sync_response", "Customise messages returned via [synchronous responses](/docs/guides/sync_responses).").WithChildren(
				docs.FieldString(
					"status",
					"Specify the status code to return with synchronous responses. This is a string value, which allows you to customize it based on resulting payloads and their metadata.",
					"200", `${! json("status") }`, `${! meta("status") }`,
				).IsInterpolated(),
				docs.FieldString("headers", "Specify headers to return with synchronous responses.").
					IsInterpolated().Map().
					HasDefault(map[string]string{
						"Content-Type": "application/octet-stream",
					}),
				docs.FieldObject("metadata_headers", "Specify criteria for which metadata values are added to the response as headers.").WithChildren(imetadata.IncludeFilterDocs()...),
			).Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewHTTPServerConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpServerInput struct {
	conf input.HTTPServerConfig
	log  log.Modular
	mgr  bundle.NewManagement

	mux     *http.ServeMux
	server  *http.Server
	timeout time.Duration

	responseStatus  *field.Expression
	responseHeaders map[string]*field.Expression
	metaFilter      *imetadata.IncludeFilter

	handlerWG    sync.WaitGroup
	transactions chan message.Transaction

	shutSig *shutdown.Signaller

	allowedVerbs map[string]struct{}

	mPostRcvd metrics.StatCounter
	mWSRcvd   metrics.StatCounter
	mLatency  metrics.StatTimer
}

func newHTTPServerInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
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

	mRcvd := mgr.Metrics().GetCounter("input_received")
	h := httpServerInput{
		shutSig:         shutdown.NewSignaller(),
		conf:            conf.HTTPServer,
		log:             mgr.Logger(),
		mgr:             mgr,
		mux:             mux,
		server:          server,
		timeout:         timeout,
		responseHeaders: map[string]*field.Expression{},
		transactions:    make(chan message.Transaction),

		allowedVerbs: verbs,

		mLatency:  mgr.Metrics().GetTimer("input_latency_ns"),
		mWSRcvd:   mRcvd,
		mPostRcvd: mRcvd,
	}

	if h.responseStatus, err = mgr.BloblEnvironment().NewField(h.conf.Response.Status); err != nil {
		return nil, fmt.Errorf("failed to parse response status expression: %v", err)
	}
	for k, v := range h.conf.Response.Headers {
		if h.responseHeaders[strings.ToLower(k)], err = mgr.BloblEnvironment().NewField(v); err != nil {
			return nil, fmt.Errorf("failed to parse response header '%v' expression: %v", k, err)
		}
	}

	if h.metaFilter, err = h.conf.Response.ExtractMetadata.CreateFilter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	postHdlr := gzipHandler(h.postHandler)
	wsHdlr := gzipHandler(h.wsHandler)
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
		if rerr := h.mgr.AccessRateLimit(r.Context(), h.conf.RateLimit, func(rl ratelimit.V1) {
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

	store := transaction.NewResultStore()
	transaction.AddResultStore(msg, store)

	h.mPostRcvd.Incr(int64(msg.Len()))
	h.log.Tracef("Consumed %v messages from POST to '%v'.\n", msg.Len(), h.conf.Path)

	resChan := make(chan error, 1)
	select {
	case h.transactions <- message.NewTransaction(msg, resChan):
	case <-time.After(h.timeout):
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
	case <-time.After(h.timeout):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-r.Context().Done():
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.shutSig.CloseNowChan():
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	responseMsg := message.QuickBatch(nil)
	for _, resMsg := range store.Get() {
		_ = resMsg.Iter(func(i int, part *message.Part) error {
			responseMsg = append(responseMsg, part)
			return nil
		})
	}
	if responseMsg.Len() > 0 {
		for k, v := range h.responseHeaders {
			headerStr, err := v.String(0, responseMsg)
			if err != nil {
				h.log.Errorf("Interpolation of response header %v error: %v", k, err)
				continue
			}
			w.Header().Set(k, headerStr)
		}

		statusCode := 200
		statusCodeStr, err := h.responseStatus.String(0, responseMsg)
		if err != nil {
			h.log.Errorf("Interpolation of response status code error: %v", err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		if statusCodeStr != "200" {
			if statusCode, err = strconv.Atoi(statusCodeStr); err != nil {
				h.log.Errorf("Failed to parse sync response status code expression: %v\n", err)
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}

		if plen := responseMsg.Len(); plen == 1 {
			part := responseMsg.Get(0)
			_ = h.metaFilter.IterStr(part, func(k, v string) error {
				w.Header().Set(k, v)
				return nil
			})
			payload := part.AsBytes()
			if w.Header().Get("Content-Type") == "" {
				w.Header().Set("Content-Type", http.DetectContentType(payload))
			}
			w.WriteHeader(statusCode)
			_, _ = w.Write(payload)
		} else if plen > 1 {
			customContentType, customContentTypeExists := h.responseHeaders["content-type"]

			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)

			var merr error
			for i := 0; i < plen && merr == nil; i++ {
				part := responseMsg.Get(i)
				_ = h.metaFilter.IterStr(part, func(k, v string) error {
					w.Header().Set(k, v)
					return nil
				})
				payload := part.AsBytes()

				mimeHeader := textproto.MIMEHeader{}
				if customContentTypeExists {
					contentTypeStr, err := customContentType.String(i, responseMsg)
					if err != nil {
						h.log.Errorf("Interpolation of content-type header error: %v", err)
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
				h.log.Errorf("Failed to return sync response: %v\n", merr)
				w.WriteHeader(http.StatusBadGateway)
			}
		}
	}
}

func (h *httpServerInput) wsHandler(w http.ResponseWriter, r *http.Request) {
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

	resChan := make(chan error, 1)
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
			h.mWSRcvd.Incr(1)
		}

		if h.conf.RateLimit != "" {
			var tUntil time.Duration
			if rerr := h.mgr.AccessRateLimit(r.Context(), h.conf.RateLimit, func(rl ratelimit.V1) {
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
				h.log.Errorf("Failed to send sync response over websocket: %v\n", err)
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

		close(h.transactions)
		h.shutSig.ShutdownComplete()
	}()

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
