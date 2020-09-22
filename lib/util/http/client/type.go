package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
	"github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

//------------------------------------------------------------------------------

// Config is a configuration struct for an HTTP client.
type Config struct {
	URL                 string            `json:"url" yaml:"url"`
	Verb                string            `json:"verb" yaml:"verb"`
	Headers             map[string]string `json:"headers" yaml:"headers"`
	CopyResponseHeaders bool              `json:"copy_response_headers" yaml:"copy_response_headers"`
	RateLimit           string            `json:"rate_limit" yaml:"rate_limit"`
	Timeout             string            `json:"timeout" yaml:"timeout"`
	Retry               string            `json:"retry_period" yaml:"retry_period"`
	MaxBackoff          string            `json:"max_retry_backoff" yaml:"max_retry_backoff"`
	NumRetries          int               `json:"retries" yaml:"retries"`
	BackoffOn           []int             `json:"backoff_on" yaml:"backoff_on"`
	DropOn              []int             `json:"drop_on" yaml:"drop_on"`
	SuccessfulOn        []int             `json:"successful_on" yaml:"successful_on"`
	TLS                 tls.Config        `json:"tls" yaml:"tls"`
	ProxyURL            string            `json:"proxy_url" yaml:"proxy_url"`
	auth.Config         `json:",inline" yaml:",inline"`
	OAuth2              auth.OAuth2Config `json:"oauth2" yaml:"oauth2"`
}

// NewConfig creates a new Config with default values.
func NewConfig() Config {
	return Config{
		URL:  "http://localhost:4195/post",
		Verb: "POST",
		Headers: map[string]string{
			"Content-Type": "application/octet-stream",
		},
		CopyResponseHeaders: false,
		RateLimit:           "",
		Timeout:             "5s",
		Retry:               "1s",
		MaxBackoff:          "300s",
		NumRetries:          3,
		BackoffOn:           []int{429},
		DropOn:              []int{},
		SuccessfulOn:        []int{},
		TLS:                 tls.NewConfig(),
		Config:              auth.NewConfig(),
		OAuth2:              auth.NewOAuth2Config(),
	}
}

//------------------------------------------------------------------------------

// Type is an output type that pushes messages to Type.
type Type struct {
	client *http.Client

	backoffOn map[int]struct{}
	dropOn    map[int]struct{}
	successOn map[int]struct{}

	url     field.Expression
	headers map[string]field.Expression
	host    field.Expression

	conf          Config
	retryThrottle *throttle.Type
	rateLimit     types.RateLimit

	log   log.Modular
	stats metrics.Type
	mgr   types.Manager

	mCount         metrics.StatCounter
	mErr           metrics.StatCounter
	mErrReq        metrics.StatCounter
	mErrReqTimeout metrics.StatCounter
	mErrRes        metrics.StatCounter
	mLimited       metrics.StatCounter
	mLimitFor      metrics.StatCounter
	mLimitErr      metrics.StatCounter
	mSucc          metrics.StatCounter
	mLatency       metrics.StatTimer

	mCodes   map[int]metrics.StatCounter
	codesMut sync.RWMutex

	ctx       context.Context
	done      func()
	closeChan <-chan struct{}
}

// New creates a new Type.
func New(conf Config, opts ...func(*Type)) (*Type, error) {
	urlStr, err := bloblang.NewField(conf.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL expression: %v", err)
	}

	h := Type{
		url:       urlStr,
		conf:      conf,
		log:       log.Noop(),
		stats:     metrics.Noop(),
		mgr:       types.NoopMgr(),
		backoffOn: map[int]struct{}{},
		dropOn:    map[int]struct{}{},
		successOn: map[int]struct{}{},
		headers:   map[string]field.Expression{},
		host:      nil,
	}
	h.ctx, h.done = context.WithCancel(context.Background())
	h.client = conf.OAuth2.Client(h.ctx)

	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if h.client.Timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if h.conf.TLS.Enabled {
		tlsConf, err := h.conf.TLS.Get()
		if err != nil {
			return nil, err
		}
		h.client.Transport = &http.Transport{
			TLSClientConfig: tlsConf,
		}
	}

	if h.conf.ProxyURL != "" {
		proxyURL, err := url.Parse(h.conf.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse proxy_url string: %v", err)
		}
		if h.client.Transport != nil {
			if tr, ok := h.client.Transport.(*http.Transport); ok {
				tr.Proxy = http.ProxyURL(proxyURL)
			} else {
				return nil, fmt.Errorf("unable to apply proxy_url to transport, unexpected type %T", h.client.Transport)
			}
		} else {
			h.client.Transport = &http.Transport{
				Proxy: http.ProxyURL(proxyURL),
			}
		}
	}

	for _, c := range conf.BackoffOn {
		h.backoffOn[c] = struct{}{}
	}
	for _, c := range conf.DropOn {
		h.dropOn[c] = struct{}{}
	}
	for _, c := range conf.SuccessfulOn {
		h.successOn[c] = struct{}{}
	}

	for k, v := range conf.Headers {
		if strings.ToLower(k) == "host" {
			if h.host, err = bloblang.NewField(v); err != nil {
				return nil, fmt.Errorf("failed to parse header 'host' expression: %v", err)
			}
		} else {
			if h.headers[k], err = bloblang.NewField(v); err != nil {
				return nil, fmt.Errorf("failed to parse header '%v' expression: %v", k, err)
			}
		}
	}

	for _, opt := range opts {
		opt(&h)
	}

	h.mCount = h.stats.GetCounter("count")
	h.mErr = h.stats.GetCounter("error")
	h.mErrReq = h.stats.GetCounter("error.request")
	h.mErrReqTimeout = h.stats.GetCounter("request_timeout")
	h.mErrRes = h.stats.GetCounter("error.response")
	h.mLimited = h.stats.GetCounter("rate_limit.count")
	h.mLimitFor = h.stats.GetCounter("rate_limit.total_ms")
	h.mLimitErr = h.stats.GetCounter("rate_limit.error")
	h.mLatency = h.stats.GetTimer("latency")
	h.mSucc = h.stats.GetCounter("success")
	h.mCodes = map[int]metrics.StatCounter{}

	if len(h.conf.RateLimit) > 0 {
		var err error
		if h.rateLimit, err = h.mgr.GetRateLimit(h.conf.RateLimit); err != nil {
			return nil, fmt.Errorf("failed to obtain rate limit resource: %v", err)
		}
	}

	var retry, maxBackoff time.Duration
	if tout := conf.Retry; len(tout) > 0 {
		var err error
		if retry, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse retry duration string: %v", err)
		}
	}
	if tout := conf.MaxBackoff; len(tout) > 0 {
		var err error
		if maxBackoff, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse max backoff duration string: %v", err)
		}
	}

	h.retryThrottle = throttle.New(
		throttle.OptMaxUnthrottledRetries(0),
		throttle.OptCloseChan(h.closeChan),
		throttle.OptThrottlePeriod(retry),
		throttle.OptMaxExponentPeriod(maxBackoff),
	)

	return &h, nil
}

//------------------------------------------------------------------------------

// OptSetCloseChan sets a channel that when closed will interrupt any blocking
// calls within the client.
func OptSetCloseChan(c <-chan struct{}) func(*Type) {
	return func(t *Type) {
		t.closeChan = c
	}
}

// OptSetLogger sets the logger to use.
func OptSetLogger(log log.Modular) func(*Type) {
	return func(t *Type) {
		t.log = log
	}
}

// OptSetStats sets the metrics aggregator to use.
func OptSetStats(stats metrics.Type) func(*Type) {
	return func(t *Type) {
		t.stats = stats
	}
}

// OptSetManager sets the manager to use.
func OptSetManager(mgr types.Manager) func(*Type) {
	return func(t *Type) {
		t.mgr = mgr
	}
}

// OptSetHTTPTransport sets the HTTP Transport to use. NOTE: This setting will
// override any configured TLS options.
//
// WARNING: DEPRECATED, use OptSetRoundTripper instead.
// TODO: V4 Remove this
func OptSetHTTPTransport(transport *http.Transport) func(*Type) {
	return func(t *Type) {
		t.client.Transport = transport
	}
}

// OptSetRoundTripper sets the *client.Transport to use for HTTP requests.
// NOTE: This setting will override any configured TLS options.
func OptSetRoundTripper(rt http.RoundTripper) func(*Type) {
	return func(t *Type) {
		t.client.Transport = rt
	}
}

//------------------------------------------------------------------------------

func (h *Type) incrCode(code int) {
	h.codesMut.RLock()
	ctr, exists := h.mCodes[code]
	h.codesMut.RUnlock()

	if exists {
		ctr.Incr(1)
		return
	}

	ctr = h.stats.GetCounter(fmt.Sprintf("code.%v", code))
	ctr.Incr(1)

	h.codesMut.Lock()
	h.mCodes[code] = ctr
	h.codesMut.Unlock()
}

func (h *Type) waitForAccess() bool {
	if h.rateLimit == nil {
		return true
	}
	for {
		period, err := h.rateLimit.Access()
		if err != nil {
			h.log.Errorf("Rate limit error: %v\n", err)
			h.mLimitErr.Incr(1)
			period = time.Second
		}
		if period > 0 {
			if err == nil {
				h.mLimited.Incr(1)
				h.mLimitFor.Incr(period.Nanoseconds() / 1000000)
			}
			select {
			case <-time.After(period):
			case <-h.closeChan:
				return false
			}
		} else {
			return true
		}
	}
}

// CreateRequest creates an HTTP request out of a single message.
func (h *Type) CreateRequest(msg types.Message) (req *http.Request, err error) {
	url := h.url.String(0, msg)

	if msg == nil || msg.Len() == 0 {
		if req, err = http.NewRequest(h.conf.Verb, url, nil); err == nil {
			for k, v := range h.headers {
				req.Header.Add(k, v.String(0, msg))
			}
			if h.host != nil {
				req.Host = h.host.String(0, msg)
			}
		}
	} else if msg.Len() == 1 {
		var body io.Reader
		if msgBytes := msg.Get(0).Get(); len(msgBytes) > 0 {
			body = bytes.NewBuffer(msgBytes)
		}
		if req, err = http.NewRequest(h.conf.Verb, url, body); err == nil {
			for k, v := range h.headers {
				req.Header.Add(k, v.String(0, msg))
			}
			if h.host != nil {
				req.Host = h.host.String(0, msg)
			}
		}
	} else {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len() && err == nil; i++ {
			contentType := "application/octet-stream"
			if v, exists := h.headers["Content-Type"]; exists {
				contentType = v.String(i, msg)
			}
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{contentType},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Get(i).Get()))
			}
		}

		writer.Close()
		if err == nil {
			if req, err = http.NewRequest(h.conf.Verb, url, body); err == nil {
				for k, v := range h.headers {
					req.Header.Add(k, v.String(0, msg))
				}
				if h.host != nil {
					req.Host = h.host.String(0, msg)
				}
				req.Header.Del("Content-Type")
				req.Header.Add("Content-Type", writer.FormDataContentType())
			}
		}
	}

	if err == nil {
		err = h.conf.Config.Sign(req)
	}
	return
}

// ParseResponse attempts to parse an HTTP response into a 2D slice of bytes.
func (h *Type) ParseResponse(res *http.Response) (resMsg types.Message, err error) {
	resMsg = message.New(nil)

	if res.Body != nil {
		defer res.Body.Close()

		contentType := res.Header.Get("Content-Type")

		var mediaType string
		var params map[string]string
		if len(contentType) > 0 {
			if mediaType, params, err = mime.ParseMediaType(res.Header.Get("Content-Type")); err != nil {
				h.mErrRes.Incr(1)
				h.mErr.Incr(1)
				h.log.Errorf("Failed to parse media type: %v\n", err)
				return
			}
		}

		var buffer bytes.Buffer
		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(res.Body, params["boundary"])
			var bufferIndex int64
			for {
				var p *multipart.Part
				if p, err = mr.NextPart(); err != nil {
					if err == io.EOF {
						err = nil
						break
					}
					return
				}

				var bytesRead int64
				if bytesRead, err = buffer.ReadFrom(p); err != nil {
					h.mErrRes.Incr(1)
					h.mErr.Incr(1)
					h.log.Errorf("Failed to read response: %v\n", err)
					return
				}

				index := resMsg.Append(message.NewPart(buffer.Bytes()[bufferIndex : bufferIndex+bytesRead]))
				bufferIndex += bytesRead

				if h.conf.CopyResponseHeaders {
					meta := resMsg.Get(index).Metadata()
					for k, values := range p.Header {
						if len(values) > 0 {
							meta.Set(strings.ToLower(k), values[0])
						}
					}
				}
			}
		} else {
			var bytesRead int64
			if bytesRead, err = buffer.ReadFrom(res.Body); err != nil {
				h.mErrRes.Incr(1)
				h.mErr.Incr(1)
				h.log.Errorf("Failed to read response: %v\n", err)
				return
			}
			if bytesRead > 0 {
				resMsg.Append(message.NewPart(buffer.Bytes()[:bytesRead]))
			} else {
				resMsg.Append(message.NewPart(nil))
			}
			if h.conf.CopyResponseHeaders {
				meta := resMsg.Get(0).Metadata()
				for k, values := range res.Header {
					if len(values) > 0 {
						meta.Set(strings.ToLower(k), values[0])
					}
				}
			}
		}
	} else {
		resMsg.Append(message.NewPart(nil))
	}

	resMsg.Iter(func(i int, p types.Part) error {
		p.Metadata().Set("http_status_code", strconv.Itoa(res.StatusCode))
		return nil
	})
	return
}

type retryStrategy int

const (
	noRetry retryStrategy = iota
	retryLinear
	retryBackoff
)

// checkStatus compares a returned status code against configured logic
// determining whether the send succeeded, and if not what the retry strategy
// should be.
func (h *Type) checkStatus(code int) (succeeded bool, retStrat retryStrategy) {
	if _, exists := h.dropOn[code]; exists {
		return false, noRetry
	}
	if _, exists := h.backoffOn[code]; exists {
		return false, retryBackoff
	}
	if _, exists := h.successOn[code]; exists {
		return true, noRetry
	}
	if code < 200 || code > 299 {
		return false, retryLinear
	}
	return true, noRetry
}

// Do attempts to create and perform an HTTP request from a message payload.
// This attempt may include retries, and if all retries fail an error is
// returned.
func (h *Type) Do(msg types.Message) (res *http.Response, err error) {
	h.mCount.Incr(1)

	var spans []opentracing.Span
	if msg != nil {
		spans = make([]opentracing.Span, msg.Len())
		msg.Iter(func(i int, p types.Part) error {
			spans[i], _ = opentracing.StartSpanFromContext(message.GetContext(p), "http_request")
			return nil
		})
		defer func() {
			for _, s := range spans {
				s.Finish()
			}
		}()
	}
	logErr := func(e error) {
		for _, s := range spans {
			s.LogFields(
				olog.String("event", "error"),
				olog.String("type", e.Error()),
			)
		}
	}

	var req *http.Request
	if req, err = h.CreateRequest(msg); err != nil {
		h.mErrReq.Incr(1)
		h.mErr.Incr(1)
		logErr(err)
		return nil, err
	}

	startedAt := time.Now()

	if !h.waitForAccess() {
		return nil, types.ErrTypeClosed
	}

	rateLimited := false
	numRetries := h.conf.NumRetries
	if res, err = h.client.Do(req); err == nil {
		h.incrCode(res.StatusCode)
		if resolved, retryStrat := h.checkStatus(res.StatusCode); !resolved {
			rateLimited = retryStrat == retryBackoff
			if retryStrat == noRetry {
				numRetries = 0
			}
			err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
			if res.Body != nil {
				res.Body.Close()
			}
		}
	} else if err, ok := err.(net.Error); ok && err.Timeout() {
		h.mErrReqTimeout.Incr(1)
	}

	i, j := 0, numRetries
	for i < j && err != nil {
		h.mErrRes.Incr(1)
		h.mErr.Incr(1)
		logErr(err)

		req, err = h.CreateRequest(msg)
		if err != nil {
			h.mErrReq.Incr(1)
			h.mErr.Incr(1)
			logErr(err)
			continue
		}
		if rateLimited {
			if !h.retryThrottle.ExponentialRetry() {
				return nil, types.ErrTypeClosed
			}
		} else {
			if !h.retryThrottle.Retry() {
				return nil, types.ErrTypeClosed
			}
		}
		if !h.waitForAccess() {
			return nil, types.ErrTypeClosed
		}
		rateLimited = false
		if res, err = h.client.Do(req); err == nil {
			h.incrCode(res.StatusCode)
			if resolved, retryStrat := h.checkStatus(res.StatusCode); !resolved {
				rateLimited = retryStrat == retryBackoff
				if retryStrat == noRetry {
					j = 0
				}
				err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
				if res.Body != nil {
					res.Body.Close()
				}
			}
		} else if err, ok := err.(net.Error); ok && err.Timeout() {
			h.mErrReqTimeout.Incr(1)
		}
		i++
	}

	if err != nil {
		h.mErrRes.Incr(1)
		h.mErr.Incr(1)
		logErr(err)
		return nil, err
	}

	h.mLatency.Timing(int64(time.Since(startedAt)))
	h.mSucc.Incr(1)
	h.retryThrottle.Reset()
	return res, nil
}

// Send attempts to send a message to an HTTP server, this attempt may include
// retries, and if all retries fail an error is returned. The message payload
// can be nil, in which case an empty body is sent. The response will be parsed
// back into a message, meaning mulitpart content handling is done for you.
//
// If the response body is empty the message returned is nil.
func (h *Type) Send(msg types.Message) (types.Message, error) {
	res, err := h.Do(msg)
	if err != nil {
		return nil, err
	}
	return h.ParseResponse(res)
}

// CloseAsync closes the HTTP client and all managed resources.
func (h *Type) CloseAsync() {
	h.done()
}

//------------------------------------------------------------------------------
