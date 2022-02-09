package http

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

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
	"github.com/Jeffail/benthos/v3/lib/util/throttle"
)

// MultipartExpressions represents three dynamic expressions that define a
// multipart message part in an HTTP request. Specifying one or more of these
// can be used as a way of creating HTTP requests that overrides the default
// behaviour.
type MultipartExpressions struct {
	ContentDisposition *field.Expression
	ContentType        *field.Expression
	Body               *field.Expression
}

// Client is a component able to send and receive Benthos messages over HTTP.
type Client struct {
	client *http.Client

	backoffOn map[int]struct{}
	dropOn    map[int]struct{}
	successOn map[int]struct{}

	url               *field.Expression
	headers           map[string]*field.Expression
	multipart         []MultipartExpressions
	host              *field.Expression
	metaInsertFilter  *metadata.IncludeFilter
	metaExtractFilter *metadata.IncludeFilter

	conf          client.Config
	retryThrottle *throttle.Type

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

	oauthClientCtx    context.Context
	oauthClientCancel func()
}

// NewClient creates a new http client that sends and receives Benthos messages.
func NewClient(conf client.Config, opts ...func(*Client)) (*Client, error) {
	h := Client{
		conf:      conf,
		log:       log.Noop(),
		stats:     metrics.Noop(),
		mgr:       mock.NewManager(),
		backoffOn: map[int]struct{}{},
		dropOn:    map[int]struct{}{},
		successOn: map[int]struct{}{},
		headers:   map[string]*field.Expression{},
		host:      nil,
	}
	h.oauthClientCtx, h.oauthClientCancel = context.WithCancel(context.Background())
	h.client = conf.OAuth2.Client(h.oauthClientCtx)

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
		if tlsConf != nil {
			if c, ok := http.DefaultTransport.(*http.Transport); ok {
				cloned := c.Clone()
				cloned.TLSClientConfig = tlsConf
				h.client.Transport = cloned
			} else {
				h.client.Transport = &http.Transport{
					TLSClientConfig: tlsConf,
				}
			}
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

	for _, opt := range opts {
		opt(&h)
	}

	var err error
	if h.url, err = interop.NewBloblangField(h.mgr, conf.URL); err != nil {
		return nil, fmt.Errorf("failed to parse URL expression: %v", err)
	}

	for k, v := range conf.Headers {
		if strings.EqualFold(k, "host") {
			if h.host, err = interop.NewBloblangField(h.mgr, v); err != nil {
				return nil, fmt.Errorf("failed to parse header 'host' expression: %v", err)
			}
		} else {
			if h.headers[k], err = interop.NewBloblangField(h.mgr, v); err != nil {
				return nil, fmt.Errorf("failed to parse header '%v' expression: %v", k, err)
			}
		}
	}

	if h.metaInsertFilter, err = h.conf.Metadata.CreateFilter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	if h.metaExtractFilter, err = h.conf.ExtractMetadata.CreateFilter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata extract filter: %w", err)
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

	if conf.RateLimit != "" {
		if err := interop.ProbeRateLimit(context.Background(), h.mgr, conf.RateLimit); err != nil {
			return nil, err
		}
	}

	h.retryThrottle = throttle.New(
		throttle.OptMaxUnthrottledRetries(0),
		throttle.OptThrottlePeriod(retry),
		throttle.OptMaxExponentPeriod(maxBackoff),
	)

	return &h, nil
}

//------------------------------------------------------------------------------

// OptSetLogger sets the logger to use.
func OptSetLogger(log log.Modular) func(*Client) {
	return func(t *Client) {
		t.log = log
	}
}

// OptSetMultiPart sets the multipart to request.
func OptSetMultiPart(multipart []MultipartExpressions) func(*Client) {
	return func(t *Client) {
		t.multipart = multipart
	}
}

// OptSetStats sets the metrics aggregator to use.
func OptSetStats(stats metrics.Type) func(*Client) {
	return func(t *Client) {
		t.stats = stats
	}
}

// OptSetManager sets the manager to use.
func OptSetManager(mgr types.Manager) func(*Client) {
	return func(t *Client) {
		t.mgr = mgr
	}
}

// OptSetRoundTripper sets the *client.Transport to use for HTTP requests.
// NOTE: This setting will override any configured TLS options.
func OptSetRoundTripper(rt http.RoundTripper) func(*Client) {
	return func(t *Client) {
		t.client.Transport = rt
	}
}

//------------------------------------------------------------------------------

func (h *Client) incrCode(code int) {
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

func (h *Client) waitForAccess(ctx context.Context) bool {
	if h.conf.RateLimit == "" {
		return true
	}
	for {
		var period time.Duration
		var err error
		if rerr := interop.AccessRateLimit(ctx, h.mgr, h.conf.RateLimit, func(rl ratelimit.V1) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			h.log.Errorf("Rate limit error: %v\n", err)
			h.mLimitErr.Incr(1)
			period = time.Second
		} else if period > 0 {
			h.mLimited.Incr(1)
			h.mLimitFor.Incr(period.Nanoseconds() / 1000000)
		}

		if period > 0 {
			select {
			case <-time.After(period):
			case <-ctx.Done():
				return false
			}
		} else {
			return true
		}
	}
}

// CreateRequest forms an *http.Request from a message to be sent as the body,
// and also a message used to form headers (they can be the same).
func (h *Client) CreateRequest(sendMsg, refMsg *message.Batch) (req *http.Request, err error) {
	var overrideContentType string
	var body io.Reader
	if len(h.multipart) > 0 {
		buf := &bytes.Buffer{}
		writer := multipart.NewWriter(buf)
		for _, v := range h.multipart {
			var part io.Writer
			mh := make(textproto.MIMEHeader)
			mh.Set("Content-Type", v.ContentType.String(0, refMsg))
			mh.Set("Content-Disposition", v.ContentDisposition.String(0, refMsg))
			if part, err = writer.CreatePart(mh); err != nil {
				return
			}
			if _, err = io.Copy(part, bytes.NewReader([]byte(v.Body.String(0, refMsg)))); err != nil {
				return
			}
		}
		writer.Close()
		overrideContentType = writer.FormDataContentType()
		body = buf
	} else if sendMsg != nil && sendMsg.Len() == 1 {
		if msgBytes := sendMsg.Get(0).Get(); len(msgBytes) > 0 {
			body = bytes.NewBuffer(msgBytes)
		}
	} else if sendMsg != nil && sendMsg.Len() > 1 {
		buf := &bytes.Buffer{}
		writer := multipart.NewWriter(buf)

		for i := 0; i < sendMsg.Len(); i++ {
			contentType := "application/octet-stream"
			if v, exists := h.headers["Content-Type"]; exists {
				contentType = v.String(i, refMsg)
			}

			headers := textproto.MIMEHeader{
				"Content-Type": []string{contentType},
			}
			_ = h.metaInsertFilter.Iter(sendMsg.Get(i), func(k, v string) error {
				headers[k] = append(headers[k], v)
				return nil
			})

			var part io.Writer
			if part, err = writer.CreatePart(headers); err != nil {
				return
			}
			if _, err = io.Copy(part, bytes.NewReader(sendMsg.Get(i).Get())); err != nil {
				return
			}
		}

		writer.Close()
		overrideContentType = writer.FormDataContentType()

		body = buf
	}

	url := h.url.String(0, refMsg)
	if req, err = http.NewRequest(h.conf.Verb, url, body); err != nil {
		return
	}

	for k, v := range h.headers {
		req.Header.Add(k, v.String(0, refMsg))
	}
	if sendMsg != nil && sendMsg.Len() == 1 {
		_ = h.metaInsertFilter.Iter(sendMsg.Get(0), func(k, v string) error {
			req.Header.Add(k, v)
			return nil
		})
	}

	if h.host != nil {
		req.Host = h.host.String(0, refMsg)
	}
	if overrideContentType != "" {
		req.Header.Del("Content-Type")
		req.Header.Add("Content-Type", overrideContentType)
	}

	err = h.conf.Config.Sign(req)
	return
}

// ParseResponse attempts to parse an HTTP response into a 2D slice of bytes.
func (h *Client) ParseResponse(res *http.Response) (resMsg *message.Batch, err error) {
	resMsg = message.QuickBatch(nil)

	if res.Body != nil {
		defer res.Body.Close()

		contentType := res.Header.Get("Content-Type")

		var mediaType string
		var params map[string]string
		if len(contentType) > 0 {
			if mediaType, params, err = mime.ParseMediaType(contentType); err != nil {
				h.log.Warnf("Failed to parse media type from Content-Type header: %v\n", err)
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

				if h.conf.CopyResponseHeaders || h.metaExtractFilter.IsSet() {
					part := resMsg.Get(index)
					for k, values := range p.Header {
						normalisedHeader := strings.ToLower(k)
						if len(values) > 0 && (h.conf.CopyResponseHeaders || h.metaExtractFilter.Match(normalisedHeader)) {
							part.MetaSet(normalisedHeader, values[0])
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
			if h.conf.CopyResponseHeaders || h.metaExtractFilter.IsSet() {
				part := resMsg.Get(0)
				for k, values := range res.Header {
					normalisedHeader := strings.ToLower(k)
					if len(values) > 0 && (h.conf.CopyResponseHeaders || h.metaExtractFilter.Match(normalisedHeader)) {
						part.MetaSet(normalisedHeader, values[0])
					}
				}
			}
		}
	} else {
		resMsg.Append(message.NewPart(nil))
	}

	_ = resMsg.Iter(func(i int, p *message.Part) error {
		p.MetaSet("http_status_code", strconv.Itoa(res.StatusCode))
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
func (h *Client) checkStatus(code int) (succeeded bool, retStrat retryStrategy) {
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

// SendToResponse attempts to create an HTTP request from a provided message,
// performs it, and then returns the *http.Response, allowing the raw response
// to be consumed.
func (h *Client) SendToResponse(ctx context.Context, sendMsg, refMsg *message.Batch) (res *http.Response, err error) {
	h.mCount.Incr(1)

	var spans []*tracing.Span
	if sendMsg != nil {
		spans = tracing.CreateChildSpans("http_request", sendMsg)
		defer func() {
			for _, s := range spans {
				s.Finish()
			}
		}()
	}
	logErr := func(e error) {
		h.mErrRes.Incr(1)
		h.mErr.Incr(1)
		for _, s := range spans {
			s.LogKV(
				"event", "error",
				"type", e.Error(),
			)
		}
	}

	var req *http.Request
	if req, err = h.CreateRequest(sendMsg, refMsg); err != nil {
		logErr(err)
		return nil, err
	}
	// Make sure we log the actual request URL
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %w", req.URL, err)
		}
	}()

	startedAt := time.Now()

	if !h.waitForAccess(ctx) {
		return nil, component.ErrTypeClosed
	}

	rateLimited := false
	numRetries := h.conf.NumRetries

	res, err = h.client.Do(req.WithContext(ctx))
	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout() {
			h.mErrReqTimeout.Incr(1)
		}
	} else {
		h.incrCode(res.StatusCode)
		if resolved, retryStrat := h.checkStatus(res.StatusCode); !resolved {
			rateLimited = retryStrat == retryBackoff
			if retryStrat == noRetry {
				numRetries = 0
			}
			err = UnexpectedErr(res)
			if res.Body != nil {
				res.Body.Close()
			}
		}
	}

	i, j := 0, numRetries
	for i < j && err != nil {
		logErr(err)
		if req, err = h.CreateRequest(sendMsg, refMsg); err != nil {
			continue
		}
		if rateLimited {
			if !h.retryThrottle.ExponentialRetryWithContext(ctx) {
				return nil, component.ErrTypeClosed
			}
		} else {
			if !h.retryThrottle.RetryWithContext(ctx) {
				return nil, component.ErrTypeClosed
			}
		}
		if !h.waitForAccess(ctx) {
			return nil, component.ErrTypeClosed
		}
		rateLimited = false
		if res, err = h.client.Do(req.WithContext(ctx)); err == nil {
			h.incrCode(res.StatusCode)
			if resolved, retryStrat := h.checkStatus(res.StatusCode); !resolved {
				rateLimited = retryStrat == retryBackoff
				if retryStrat == noRetry {
					j = 0
				}
				err = UnexpectedErr(res)
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
		logErr(err)
		return nil, err
	}

	h.mLatency.Timing(int64(time.Since(startedAt)))
	h.mSucc.Incr(1)
	h.retryThrottle.Reset()
	return res, nil
}

// UnexpectedErr get error body
func UnexpectedErr(res *http.Response) error {
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	return component.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status, Body: body}
}

// Send creates an HTTP request from the client config, a provided message to be
// sent as the body of the request, and a reference message used to establish
// interpolated fields for the request (which can be the same as the message
// used for the body).
//
// If the request is successful then the response is parsed into a message,
// including headers added as metadata (when configured to do so).
func (h *Client) Send(ctx context.Context, sendMsg, refMsg *message.Batch) (*message.Batch, error) {
	res, err := h.SendToResponse(ctx, sendMsg, refMsg)
	if err != nil {
		return nil, err
	}
	return h.ParseResponse(res)
}

// Close the client.
func (h *Client) Close(ctx context.Context) error {
	h.oauthClientCancel()
	return nil
}
