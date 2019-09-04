// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package input

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	httputil "github.com/Jeffail/benthos/v3/lib/util/http"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"github.com/gorilla/websocket"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPServer] = TypeSpec{
		constructor: NewHTTPServer,
		description: `
Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS,
which is enabled when key and cert files are specified.

You can leave the 'address' config field blank in order to use the instance wide
HTTP server.

The field ` + "`rate_limit`" + ` allows you to specify an optional
` + "[`rate_limit` resource](../rate_limits/README.md)" + `, which will be
applied to each HTTP request made and each websocket payload received.

When the rate limit is breached HTTP requests will have a 429 response returned
with a Retry-After header. Websocket payloads will be dropped and an optional
response payload will be sent as per ` + "`ws_rate_limit_message`" + `.

### Responses

It's possible to return a response for each message received using
[synchronous responses](../sync_responses.md).

### Endpoints

The following fields specify endpoints that are registered for sending messages:

#### ` + "`path` (defaults to `/post`)" + `

This endpoint expects POST requests where the entire request body is consumed as
a single message.

If the request contains a multipart ` + "`content-type`" + ` header as per
[rfc1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html) then the
multiple parts are consumed as a batch of messages, where each body part is a
message of the batch.

#### ` + "`ws_path` (defaults to `/post/ws`)" + `

Creates a websocket connection, where payloads received on the socket are passed
through the pipeline as a batch of one message.

You may specify an optional ` + "`ws_welcome_message`" + `, which is a static
payload to be sent to all clients once a websocket connection is first
established.

It's also possible to specify a ` + "`ws_rate_limit_message`" + `, which is a
static payload to be sent to clients that have triggered the servers rate limit.

### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- http_server_user_agent
- All headers (only first values are taken)
- All query parameters
- All cookies
` + "```" + `

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).`,
	}
}

//------------------------------------------------------------------------------

// HTTPServerConfig contains configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address            string `json:"address" yaml:"address"`
	Path               string `json:"path" yaml:"path"`
	WSPath             string `json:"ws_path" yaml:"ws_path"`
	WSWelcomeMessage   string `json:"ws_welcome_message" yaml:"ws_welcome_message"`
	WSRateLimitMessage string `json:"ws_rate_limit_message" yaml:"ws_rate_limit_message"`
	Timeout            string `json:"timeout" yaml:"timeout"`
	RateLimit          string `json:"rate_limit" yaml:"rate_limit"`
	CertFile           string `json:"cert_file" yaml:"cert_file"`
	KeyFile            string `json:"key_file" yaml:"key_file"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:            "",
		Path:               "/post",
		WSPath:             "/post/ws",
		WSWelcomeMessage:   "",
		WSRateLimitMessage: "",
		Timeout:            "5s",
		RateLimit:          "",
		CertFile:           "",
		KeyFile:            "",
	}
}

//------------------------------------------------------------------------------

// HTTPServer is an input type that registers a range of HTTP endpoints where
// requests can send messages through Benthos. The endpoints are registered on
// the general Benthos HTTP server by default. It is also possible to specify a
// custom address to bind a new server to which the endpoints will be registered
// on instead.
type HTTPServer struct {
	running int32

	conf  Config
	stats metrics.Type
	log   log.Modular

	ratelimit types.RateLimit

	mux     *http.ServeMux
	server  *http.Server
	timeout time.Duration

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}

	mCount         metrics.StatCounter
	mRateLimited   metrics.StatCounter
	mWSRateLimited metrics.StatCounter
	mRcvd          metrics.StatCounter
	mPartsRcvd     metrics.StatCounter
	mSyncCount     metrics.StatCounter
	mSyncErr       metrics.StatCounter
	mSyncSucc      metrics.StatCounter
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

	if len(conf.HTTPServer.Address) > 0 {
		mux = http.NewServeMux()
		server = &http.Server{Addr: conf.HTTPServer.Address, Handler: mux}
	}

	var timeout time.Duration
	if len(conf.HTTPServer.Timeout) > 0 {
		var err error
		if timeout, err = time.ParseDuration(conf.HTTPServer.Timeout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	var ratelimit types.RateLimit
	if len(conf.HTTPServer.RateLimit) > 0 {
		var err error
		if ratelimit, err = mgr.GetRateLimit(conf.HTTPServer.RateLimit); err != nil {
			return nil, fmt.Errorf("unable to locate rate_limit resource '%v': %v", conf.HTTPServer.RateLimit, err)
		}
	}

	h := HTTPServer{
		running:      1,
		conf:         conf,
		stats:        stats,
		log:          log,
		mux:          mux,
		ratelimit:    ratelimit,
		server:       server,
		timeout:      timeout,
		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),

		mCount:         stats.GetCounter("count"),
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

	postHdlr := httputil.GzipHandler(h.postHandler)
	wsHdlr := httputil.GzipHandler(h.wsHandler)
	if mux != nil {
		if len(h.conf.HTTPServer.Path) > 0 {
			mux.HandleFunc(h.conf.HTTPServer.Path, postHdlr)
		}
		if len(h.conf.HTTPServer.WSPath) > 0 {
			mux.HandleFunc(h.conf.HTTPServer.WSPath, wsHdlr)
		}
	} else {
		if len(h.conf.HTTPServer.Path) > 0 {
			mgr.RegisterEndpoint(
				h.conf.HTTPServer.Path, "Post a message into Benthos.", postHdlr,
			)
		}
		if len(h.conf.HTTPServer.WSPath) > 0 {
			mgr.RegisterEndpoint(
				h.conf.HTTPServer.WSPath, "Post messages via websocket into Benthos.", wsHdlr,
			)
		}
	}

	go h.loop()
	return &h, nil
}

//------------------------------------------------------------------------------

func extractMessageFromRequest(r *http.Request) (types.Message, error) {
	msg := message.New(nil)

	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			var p *multipart.Part
			if p, err = mr.NextPart(); err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return nil, err
			}
			var msgBytes []byte
			if msgBytes, err = ioutil.ReadAll(p); err != nil {
				return nil, err
			}
			msg.Append(message.NewPart(msgBytes))
		}
	} else {
		var msgBytes []byte
		if msgBytes, err = ioutil.ReadAll(r.Body); err != nil {
			return nil, err
		}
		msg.Append(message.NewPart(msgBytes))
	}

	meta := metadata.New(nil)
	meta.Set("http_server_user_agent", r.UserAgent())
	for k, v := range r.Header {
		if len(v) > 0 {
			meta.Set(k, v[0])
		}
	}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			meta.Set(k, v[0])
		}
	}
	for _, c := range r.Cookies() {
		meta.Set(c.Name, c.Value)
	}
	message.SetAllMetadata(msg, meta)

	// Try to either extract parent span from headers, or create a new one.
	carrier := opentracing.HTTPHeadersCarrier(r.Header)
	if clientSpanContext, serr := opentracing.GlobalTracer().Extract(opentracing.HTTPHeaders, carrier); serr == nil {
		tracing.InitSpansFromParent("input_http_server_post", clientSpanContext, msg)
	} else {
		tracing.InitSpans("input_http_server_post", msg)
	}

	return msg, nil
}

func (h *HTTPServer) postHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if atomic.LoadInt32(&h.running) != 1 {
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	if h.ratelimit != nil {
		if tUntil, err := h.ratelimit.Access(); err != nil {
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

	var err error
	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warnf("Request read failed: %v\n", err)
			return
		}
	}()

	msg, err := extractMessageFromRequest(r)
	if err != nil {
		return
	}
	defer tracing.FinishSpans(msg)

	store := roundtrip.NewResultStore()
	roundtrip.AddResultStore(msg, store)

	h.mCount.Incr(1)
	h.mPartsRcvd.Incr(int64(msg.Len()))
	h.mRcvd.Incr(1)

	resChan := make(chan types.Response)
	select {
	case h.transactions <- types.NewTransaction(msg, resChan):
	case <-time.After(h.timeout):
		h.mTimeout.Incr(1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.closeChan:
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
		h.mSucc.Incr(1)
	case <-time.After(h.timeout):
		h.mTimeout.Incr(1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		go func() {
			// Even if the request times out, we still need to drain a response.
			resAsync := <-resChan
			if resAsync.Error() != nil {
				h.mAsyncErr.Incr(1)
				h.mErr.Incr(1)
			} else {
				h.mAsyncSucc.Incr(1)
				h.mSucc.Incr(1)
			}
		}()
		return
	}

	var parts []types.Part
	for _, responseMsg := range store.Get() {
		responseMsg.Iter(func(i int, part types.Part) error {
			parts = append(parts, part)
			return nil
		})
	}
	if plen := len(parts); plen == 1 {
		payload := parts[0].Get()
		w.Header().Set("Content-Type", http.DetectContentType(payload))
		w.Write(payload)
	} else if plen > 1 {
		writer := multipart.NewWriter(w)
		var merr error
		for i := 0; i < plen && merr == nil; i++ {
			payload := parts[i].Get()

			var part io.Writer
			if part, merr = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{http.DetectContentType(payload)},
			}); merr == nil {
				_, merr = io.Copy(part, bytes.NewReader(payload))
			}
		}
		if merr != nil {
			h.log.Errorf("Failed to return sync response: %v\n", merr)
		}
	}

	return
}

func (h *HTTPServer) wsHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warnf("Websocket request failed: %v\n", err)
			return
		}
	}()

	upgrader := websocket.Upgrader{}

	var ws *websocket.Conn
	if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
		return
	}
	defer ws.Close()

	resChan := make(chan types.Response)
	throt := throttle.New(throttle.OptCloseChan(h.closeChan))

	if welMsg := h.conf.HTTPServer.WSWelcomeMessage; len(welMsg) > 0 {
		if err = ws.WriteMessage(websocket.BinaryMessage, []byte(welMsg)); err != nil {
			h.log.Errorf("Failed to send welcome message: %v\n", err)
		}
	}

	var msgBytes []byte
	for atomic.LoadInt32(&h.running) == 1 {
		if msgBytes == nil {
			if _, msgBytes, err = ws.ReadMessage(); err != nil {
				return
			}
			h.mWSCount.Incr(1)
			h.mCount.Incr(1)
		}

		if h.ratelimit != nil {
			if tUntil, err := h.ratelimit.Access(); err != nil || tUntil > 0 {
				if err != nil {
					h.log.Warnf("Failed to access rate limit: %v\n", err)
				}
				if rlMsg := h.conf.HTTPServer.WSRateLimitMessage; len(rlMsg) > 0 {
					if err = ws.WriteMessage(websocket.BinaryMessage, []byte(rlMsg)); err != nil {
						h.log.Errorf("Failed to send rate limit message: %v\n", err)
					}
				}
				h.mWSRateLimited.Incr(1)
				continue
			}
		}

		msg := message.New([][]byte{msgBytes})

		meta := msg.Get(0).Metadata()
		meta.Set("http_server_user_agent", r.UserAgent())
		for k, v := range r.Header {
			if len(v) > 0 {
				meta.Set(k, v[0])
			}
		}
		for k, v := range r.URL.Query() {
			if len(v) > 0 {
				meta.Set(k, v[0])
			}
		}
		for _, c := range r.Cookies() {
			meta.Set(c.Name, c.Value)
		}
		tracing.InitSpans("input_http_server_websocket", msg)

		store := roundtrip.NewResultStore()
		roundtrip.AddResultStore(msg, store)

		select {
		case h.transactions <- types.NewTransaction(msg, resChan):
		case <-h.closeChan:
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
				h.mWSSucc.Incr(1)
				h.mSucc.Incr(1)
				msgBytes = nil
				throt.Reset()
			}
		case <-h.closeChan:
			return
		}

		for _, responseMsg := range store.Get() {
			if err := responseMsg.Iter(func(i int, part types.Part) error {
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
		atomic.StoreInt32(&h.running, 0)

		if h.server != nil {
			h.server.Shutdown(context.Background())
		}

		mRunning.Decr(1)

		close(h.transactions)
		close(h.closedChan)
	}()
	mRunning.Incr(1)

	if h.server != nil {
		go func() {
			if len(h.conf.HTTPServer.KeyFile) > 0 || len(h.conf.HTTPServer.CertFile) > 0 {
				h.log.Infof(
					"Receiving HTTPS messages at: https://%s\n",
					h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
				)
				if err := h.server.ListenAndServeTLS(
					h.conf.HTTPServer.CertFile, h.conf.HTTPServer.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			} else {
				h.log.Infof(
					"Receiving HTTP messages at: http://%s\n",
					h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
				)
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			}
		}()
	}

	<-h.closeChan
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
	if atomic.CompareAndSwapInt32(&h.running, 1, 0) {
		close(h.closeChan)
	}
}

// WaitForClose blocks until the HTTPServer input has closed down.
func (h *HTTPServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
