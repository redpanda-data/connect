package httpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/old/util/throttle"
	"github.com/benthosdev/benthos/v4/internal/tracing/v2"
	"github.com/benthosdev/benthos/v4/public/service"
)

// Client is a component able to send and receive Benthos messages over HTTP.
type Client struct {
	reqCreator *RequestCreator

	// Client creator
	client       *http.Client
	clientCtx    context.Context
	clientCancel func()

	// Request execution and retry logic
	rateLimit     string
	numRetries    int
	retryThrottle *throttle.Type
	backoffOn     map[int]struct{}
	dropOn        map[int]struct{}
	successOn     map[int]struct{}

	// Response extraction
	metaExtractFilter *service.MetadataFilter

	// Observability
	log *service.Logger
	mgr *service.Resources

	mLatency *service.MetricTimer
	mCodes   map[int]*service.MetricCounter
	codesMut sync.RWMutex
}

// NewClientFromOldConfig creates a new request creator from an old struct style
// config. Eventually I'd like to phase these out for the more dynamic service
// style parses, but it'll take a while so we have this for now.
func NewClientFromOldConfig(conf OldConfig, mgr *service.Resources, opts ...RequestOpt) (*Client, error) {
	reqCreator, err := RequestCreatorFromOldConfig(conf, mgr, opts...)
	if err != nil {
		return nil, err
	}

	h := Client{
		reqCreator:        reqCreator,
		client:            &http.Client{},
		metaExtractFilter: conf.ExtractMetadata,

		backoffOn: map[int]struct{}{},
		dropOn:    map[int]struct{}{},
		successOn: map[int]struct{}{},

		mgr: mgr,
		log: mgr.Logger(),
	}
	h.clientCtx, h.clientCancel = context.WithCancel(context.Background())

	if conf.Timeout > 0 {
		h.client.Timeout = conf.Timeout
	}

	if conf.TLSEnabled && conf.TLSConf != nil {
		if c, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := c.Clone()
			cloned.TLSClientConfig = conf.TLSConf
			h.client.Transport = cloned
		} else {
			h.client.Transport = &http.Transport{
				TLSClientConfig: conf.TLSConf,
			}
		}
	}

	if conf.ProxyURL != "" {
		proxyURL, err := url.Parse(conf.ProxyURL)
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

	h.client.Transport, err = newRequestLog(h.client.Transport, h.log, conf.DumpRequestLogLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to config logger for request dump: %v", err)
	}

	h.client = conf.OAuth2.Client(h.clientCtx, h.client)

	for _, c := range conf.BackoffOn {
		h.backoffOn[c] = struct{}{}
	}
	for _, c := range conf.DropOn {
		h.dropOn[c] = struct{}{}
	}
	for _, c := range conf.SuccessfulOn {
		h.successOn[c] = struct{}{}
	}

	h.mLatency = h.mgr.Metrics().NewTimer("http_request_latency_ns")
	h.mCodes = map[int]*service.MetricCounter{}

	if h.rateLimit = conf.RateLimit; h.rateLimit != "" {
		if !h.mgr.HasRateLimit(h.rateLimit) {
			return nil, fmt.Errorf("rate limit resource '%v' was not found", h.rateLimit)
		}
	}

	h.numRetries = conf.NumRetries
	h.retryThrottle = throttle.New(
		throttle.OptMaxUnthrottledRetries(0),
		throttle.OptThrottlePeriod(conf.Retry),
		throttle.OptMaxExponentPeriod(conf.MaxBackoff),
	)

	return &h, nil
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

	tier := code / 100
	if tier < 0 || tier > 5 {
		return
	}
	ctr = h.mgr.Metrics().NewCounter(fmt.Sprintf("http_request_code_%vxx", tier))
	ctr.Incr(1)

	h.codesMut.Lock()
	h.mCodes[code] = ctr
	h.codesMut.Unlock()
}

func (h *Client) waitForAccess(ctx context.Context) bool {
	if h.rateLimit == "" {
		return true
	}
	for {
		var period time.Duration
		var err error
		if rerr := h.mgr.AccessRateLimit(ctx, h.rateLimit, func(rl service.RateLimit) {
			period, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			h.log.Errorf("Rate limit error: %v\n", err)
			period = time.Second
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

// ResponseToBatch attempts to parse an HTTP response into a 2D slice of bytes.
func (h *Client) ResponseToBatch(res *http.Response) (service.MessageBatch, error) {
	var resMsg service.MessageBatch

	annotatePart := func(p *service.Message) {
		p.MetaSetMut("http_status_code", res.StatusCode)
		if !h.metaExtractFilter.IsEmpty() {
			for k, values := range res.Header {
				normalisedHeader := strings.ToLower(k)
				if len(values) > 0 && h.metaExtractFilter.Match(normalisedHeader) {
					p.MetaSetMut(normalisedHeader, values[0])
				}
			}
		}
	}

	if res.Body == nil {
		nextPart := service.NewMessage(nil)
		annotatePart(nextPart)
		resMsg = append(resMsg, nextPart)
		return resMsg, nil
	}
	defer res.Body.Close()

	var mediaType string
	var params map[string]string
	var err error
	if contentType := res.Header.Get("Content-Type"); contentType != "" {
		if mediaType, params, err = mime.ParseMediaType(contentType); err != nil {
			h.log.Warnf("Failed to parse media type from Content-Type header: %v\n", err)
		}
	}

	var buffer bytes.Buffer
	if !strings.HasPrefix(mediaType, "multipart/") {
		var bytesRead int64
		if bytesRead, err = buffer.ReadFrom(res.Body); err != nil {
			h.log.Errorf("Failed to read response: %v\n", err)
			return resMsg, err
		}

		nextPart := service.NewMessage(nil)
		if bytesRead > 0 {
			nextPart.SetBytes(buffer.Bytes()[:bytesRead])
		}

		annotatePart(nextPart)
		resMsg = append(resMsg, nextPart)
		return resMsg, nil
	}

	mr := multipart.NewReader(res.Body, params["boundary"])
	var bufferIndex int64
	for {
		var p *multipart.Part
		if p, err = mr.NextPart(); err != nil {
			if err == io.EOF {
				break
			}
			return resMsg, err
		}

		var bytesRead int64
		if bytesRead, err = buffer.ReadFrom(p); err != nil {
			h.log.Errorf("Failed to read response: %v\n", err)
			return resMsg, err
		}

		nextPart := service.NewMessage(buffer.Bytes()[bufferIndex : bufferIndex+bytesRead])
		bufferIndex += bytesRead

		annotatePart(nextPart)
		resMsg = append(resMsg, nextPart)
	}

	return resMsg, nil
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
func (h *Client) SendToResponse(ctx context.Context, sendMsg service.MessageBatch) (res *http.Response, err error) {
	var spans []*tracing.Span
	if sendMsg != nil {
		sendMsg, spans = tracing.WithChildSpans(h.mgr.OtelTracer(), "http_request", sendMsg)
		defer func() {
			for _, s := range spans {
				s.Finish()
			}
		}()
	}
	logErr := func(e error) {
		for _, s := range spans {
			s.LogKV(
				"event", "error",
				"type", e.Error(),
			)
		}
	}

	var req *http.Request
	if req, err = h.reqCreator.Create(sendMsg); err != nil {
		logErr(err)
		return nil, err
	}
	// Make sure we log the actual request URL
	defer func() {
		if err != nil {
			err = fmt.Errorf("%s: %w", req.URL, err)
		}
	}()

	if !h.waitForAccess(ctx) {
		return nil, component.ErrTypeClosed
	}

	rateLimited := false
	numRetries := h.numRetries

	startedAt := time.Now()
	if res, err = h.client.Do(req.WithContext(ctx)); err == nil {
		h.incrCode(res.StatusCode)
		if resolved, retryStrat := h.checkStatus(res.StatusCode); !resolved {
			rateLimited = retryStrat == retryBackoff
			if retryStrat == noRetry {
				numRetries = 0
			}
			err = unexpectedErr(res)
			if res.Body != nil {
				res.Body.Close()
			}
		}
	}
	h.mLatency.Timing(time.Since(startedAt).Nanoseconds())

	i, j := 0, numRetries
	for i < j && err != nil {
		logErr(err)
		if req, err = h.reqCreator.Create(sendMsg); err != nil {
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

		startedAt = time.Now()
		if res, err = h.client.Do(req.WithContext(ctx)); err == nil {
			h.incrCode(res.StatusCode)
			if resolved, retryStrat := h.checkStatus(res.StatusCode); !resolved {
				rateLimited = retryStrat == retryBackoff
				if retryStrat == noRetry {
					j = 0
				}
				err = unexpectedErr(res)
				if res.Body != nil {
					res.Body.Close()
				}
			}
		}
		h.mLatency.Timing(time.Since(startedAt).Nanoseconds())
		i++
	}
	if err != nil {
		logErr(err)
		return nil, err
	}

	h.retryThrottle.Reset()
	return res, nil
}

func unexpectedErr(res *http.Response) error {
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
func (h *Client) Send(ctx context.Context, sendMsg service.MessageBatch) (service.MessageBatch, error) {
	res, err := h.SendToResponse(ctx, sendMsg)
	if err != nil {
		return nil, err
	}
	return h.ResponseToBatch(res)
}

// Close the client.
func (h *Client) Close(ctx context.Context) error {
	h.clientCancel()
	return nil
}
