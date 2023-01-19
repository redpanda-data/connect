package httpclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/benthosdev/benthos/v4/internal/log"

	"go.uber.org/multierr"
)

type roundTripper struct {
	base   http.RoundTripper
	logger log.Modular
	level  string
}

var _ http.RoundTripper = (*roundTripper)(nil)

func newRequestLog(base http.RoundTripper, logger log.Modular, lvl string) (http.RoundTripper, error) {
	if base == nil {
		base = http.DefaultTransport
	}

	if lvl == "" {
		return base, nil
	}

	if logger == nil {
		return nil, fmt.Errorf("logger on dump_request_log is not configured")
	}

	return &roundTripper{
		base:   base,
		logger: logger,
		level:  strings.ToUpper(strings.TrimSpace(lvl)),
	}, nil
}

func (r *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	t0 := time.Now()

	var (
		respOriginal *http.Response // final response
		errCum       error          // final error
	)

	var (
		reqBodyCaptured interface{}
		reqBodyBuf      = &bytes.Buffer{}
		reqBodyErr      error
	)

	if req != nil && req.Body != nil {
		_, reqBodyErr = io.Copy(reqBodyBuf, req.Body)
		if reqBodyErr != nil {
			errCum = multierr.Append(errCum, fmt.Errorf("error copy request body: %w", reqBodyErr))
			reqBodyBuf = &bytes.Buffer{}
		}

		if _err := req.Body.Close(); _err != nil {
			errCum = multierr.Append(errCum, fmt.Errorf("error closing request body: %w", _err))
		}

		req.Body = io.NopCloser(reqBodyBuf)
	}

	// use json.Unmarshal instead of json.NewDecoder to make sure we can re-read the buffer
	if _err := json.Unmarshal(reqBodyBuf.Bytes(), &reqBodyCaptured); _err != nil && reqBodyBuf.Len() > 0 {
		reqBodyCaptured = reqBodyBuf.String()
	}

	var roundTripErr error
	respOriginal, roundTripErr = r.base.RoundTrip(req)
	if roundTripErr != nil {
		errCum = multierr.Append(errCum, fmt.Errorf("error doing actual request: %w", roundTripErr))
	}

	var (
		respBodyCaptured interface{}
		respBodyBuf      = &bytes.Buffer{}
		respErrBody      error
	)

	if respOriginal != nil && respOriginal.Body != nil {
		_, respErrBody = io.Copy(respBodyBuf, respOriginal.Body)
		if respErrBody != nil {
			errCum = multierr.Append(errCum, fmt.Errorf("error copy response body: %w", respErrBody))
			respBodyBuf = &bytes.Buffer{}
		}

		if _err := respOriginal.Body.Close(); _err != nil {
			errCum = multierr.Append(errCum, fmt.Errorf("error closing response body: %w", _err))
		}

		respOriginal.Body = io.NopCloser(respBodyBuf)
	}

	// use json.Unmarshal instead of json.NewDecoder to make sure we can re-read the buffer
	if _err := json.Unmarshal(respBodyBuf.Bytes(), &respBodyCaptured); _err != nil && respBodyBuf.Len() > 0 {
		respBodyCaptured = respBodyBuf.String()
	}

	// log outgoing request as simple map
	accessLog := map[string]any{
		"elapsed_ns": time.Since(t0).Nanoseconds(),
	}

	// append to map only when the http.Request is not nil
	if req != nil {
		accessLog["request"] = map[string]any{
			"url":    req.URL.Redacted(),
			"method": req.Method,
			"header": toSimpleMap(req.Header),
			"body":   reqBodyCaptured,
		}
	}

	// append to map only when the http.Response is not nil
	if respOriginal != nil {
		accessLog["response"] = map[string]any{
			"status_code":    respOriginal.StatusCode,
			"content_length": respOriginal.ContentLength,
			"header":         toSimpleMap(respOriginal.Header),
			"body":           respBodyCaptured,
		}
	}

	// append error if any
	if errCum != nil {
		accessLog["error"] = errCum.Error()
	}

	logger := r.logger.With("access_log", accessLog)
	msg := "http request log"

	switch r.level {
	case "TRACE":
		logger.Traceln(msg)
	case "DEBUG":
		logger.Debugln(msg)
	case "INFO":
		logger.Infoln(msg)
	case "WARN":
		logger.Warnln(msg)
	case "ERROR":
		logger.Errorln(msg)
	case "FATAL":
		logger.Fatalln(msg)
	}

	return respOriginal, roundTripErr
}

var toSimpleMap = func(h http.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		out[k] = strings.Join(v, " ")
	}

	return out
}
