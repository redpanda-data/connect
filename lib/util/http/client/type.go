// Copyright (c) 2018 Ashley Jeffs
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

package client

import (
	"bytes"
	"crypto/tls"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/auth"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

// Config is a configuration struct for an HTTP client.
type Config struct {
	URL            string            `json:"url" yaml:"url"`
	Verb           string            `json:"verb" yaml:"verb"`
	Headers        map[string]string `json:"headers" yaml:"headers"`
	TimeoutMS      int64             `json:"timeout_ms" yaml:"timeout_ms"`
	RetryMS        int64             `json:"retry_period_ms" yaml:"retry_period_ms"`
	MaxBackoffMS   int64             `json:"max_retry_backoff_ms" yaml:"max_retry_backoff_ms"`
	NumRetries     int               `json:"retries" yaml:"retries"`
	BackoffOn      []int             `json:"backoff_on" yaml:"backoff_on"`
	DropOn         []int             `json:"drop_on" yaml:"drop_on"`
	SkipCertVerify bool              `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	auth.Config    `json:",inline" yaml:",inline"`
}

// NewConfig creates a new Config with default values.
func NewConfig() Config {
	return Config{
		URL:  "http://localhost:4195/post",
		Verb: "POST",
		Headers: map[string]string{
			"Content-Type": "application/octet-stream",
		},
		TimeoutMS:      5000,
		RetryMS:        1000,
		MaxBackoffMS:   300000,
		NumRetries:     3,
		BackoffOn:      []int{429},
		DropOn:         []int{},
		SkipCertVerify: false,
		Config:         auth.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// Type is an output type that pushes messages to Type.
type Type struct {
	client http.Client

	backoffOn map[int]struct{}
	dropOn    map[int]struct{}

	URL           string
	conf          Config
	retryThrottle *throttle.Type

	closeChan <-chan struct{}
}

// New creates a new Type.
func New(conf Config, opts ...func(*Type)) *Type {
	h := Type{
		URL:       conf.URL,
		conf:      conf,
		backoffOn: map[int]struct{}{},
		dropOn:    map[int]struct{}{},
	}

	h.client.Timeout = time.Duration(h.conf.TimeoutMS) * time.Millisecond
	if h.conf.SkipCertVerify {
		h.client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	for _, c := range conf.BackoffOn {
		h.backoffOn[c] = struct{}{}
	}
	for _, c := range conf.DropOn {
		h.dropOn[c] = struct{}{}
	}

	for _, opt := range opts {
		opt(&h)
	}

	h.retryThrottle = throttle.New(
		throttle.OptMaxUnthrottledRetries(0),
		throttle.OptCloseChan(h.closeChan),
		throttle.OptThrottlePeriod(time.Millisecond*time.Duration(conf.RetryMS)),
		throttle.OptMaxExponentPeriod(time.Millisecond*time.Duration(conf.MaxBackoffMS)),
	)

	return &h
}

// OptSetCloseChan sets a channel that when closed will interrupt any blocking
// calls within the client.
func OptSetCloseChan(c <-chan struct{}) func(*Type) {
	return func(t *Type) {
		t.closeChan = c
	}
}

//------------------------------------------------------------------------------

// CreateRequest creates an HTTP request out of a single message.
func (h *Type) CreateRequest(msg types.Message) (req *http.Request, err error) {
	if msg == nil || msg.Len() == 0 {
		if req, err = http.NewRequest(
			h.conf.Verb,
			h.URL,
			nil,
		); err == nil {
			for k, v := range h.conf.Headers {
				req.Header.Add(k, v)
			}
		}
	} else if msg.Len() == 1 {
		body := bytes.NewBuffer(msg.GetAll()[0])
		if req, err = http.NewRequest(
			h.conf.Verb,
			h.URL,
			body,
		); err == nil {
			for k, v := range h.conf.Headers {
				req.Header.Add(k, v)
			}
		}
	} else {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len() && err == nil; i++ {
			contentType := "application/octet-stream"
			if v, exists := h.conf.Headers["Content-Type"]; exists {
				contentType = v
			}
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{contentType},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Get(i)))
			}
		}

		writer.Close()
		if req, err = http.NewRequest(
			h.conf.Verb,
			h.URL,
			body,
		); err == nil {
			for k, v := range h.conf.Headers {
				req.Header.Add(k, v)
			}
			req.Header.Del("Content-Type")
			req.Header.Add("Content-Type", writer.FormDataContentType())
		}
	}
	err = h.conf.Config.Sign(req)
	return
}

// ParseResponse attempts to parse an HTTP response into a 2D slice of bytes.
func (h *Type) ParseResponse(res *http.Response) (resMsg types.Message, err error) {
	if res.Body == nil {
		return nil, nil
	}
	contentType := res.Header.Get("Content-Type")

	var mediaType string
	var params map[string]string
	if len(contentType) > 0 {
		if mediaType, params, err = mime.ParseMediaType(res.Header.Get("Content-Type")); err != nil {
			return
		}
	}

	var buffer bytes.Buffer
	if strings.HasPrefix(mediaType, "multipart/") {
		resMsg = types.NewMessage(nil)

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
				return
			}

			resMsg.Append(buffer.Bytes()[bufferIndex : bufferIndex+bytesRead])
			bufferIndex += bytesRead
		}
	} else {
		var bytesRead int64
		if bytesRead, err = buffer.ReadFrom(res.Body); err != nil {
			return
		}
		if bytesRead > 0 {
			resMsg = types.NewMessage([][]byte{buffer.Bytes()[:bytesRead]})
		}
	}

	res.Body.Close()
	return
}

// checkStatus compares a returned status code against configured logic
// determining whether the send is resolved, and if not whether the retry should
// be linear.
func (h *Type) checkStatus(code int) (resolved bool, linearRetry bool) {
	if _, exists := h.dropOn[code]; exists {
		return true, false
	}
	if _, exists := h.backoffOn[code]; exists {
		return false, false
	}
	if code < 200 || code > 299 {
		return false, true
	}
	return true, false
}

// Do attempts to create and perform an HTTP request from a message payload.
// This attempt may include retries, and if all retries fail an error is
// returned.
func (h *Type) Do(msg types.Message) (res *http.Response, err error) {
	var req *http.Request
	if req, err = h.CreateRequest(msg); err != nil {
		return nil, err
	}

	rateLimited := false
	if res, err = h.client.Do(req); err == nil {
		if resolved, linear := h.checkStatus(res.StatusCode); !resolved {
			rateLimited = !linear
			err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
			if res.Body != nil {
				res.Body.Close()
			}
		}
	}

	i, j := 0, h.conf.NumRetries
	for i < j && err != nil {
		req, err = h.CreateRequest(msg)
		if err != nil {
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
		rateLimited = false
		if res, err = h.client.Do(req); err == nil {
			if resolved, linear := h.checkStatus(res.StatusCode); !resolved {
				rateLimited = !linear
				err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
				if res.Body != nil {
					res.Body.Close()
				}
			}
		}
		i++
	}

	if err != nil {
		return nil, err
	}

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

//------------------------------------------------------------------------------
