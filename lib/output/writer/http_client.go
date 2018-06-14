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

package writer

import (
	"bytes"
	"crypto/tls"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"time"

	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/auth"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/throttle"
)

//------------------------------------------------------------------------------

// HTTPClientConfig is configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	URL            string `json:"url" yaml:"url"`
	Verb           string `json:"verb" yaml:"verb"`
	ContentType    string `json:"content_type" yaml:"content_type"`
	TimeoutMS      int64  `json:"timeout_ms" yaml:"timeout_ms"`
	RetryMS        int64  `json:"retry_period_ms" yaml:"retry_period_ms"`
	MaxBackoffMS   int64  `json:"max_retry_backoff_ms" yaml:"max_retry_backoff_ms"`
	NumRetries     int    `json:"retries" yaml:"retries"`
	BackoffOn      []int  `json:"backoff_on" yaml:"backoff_on"`
	DropOn         []int  `json:"drop_on" yaml:"drop_on"`
	SkipCertVerify bool   `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	auth.Config    `json:",inline" yaml:",inline"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		URL:            "http://localhost:4195/post",
		Verb:           "POST",
		ContentType:    "application/octet-stream",
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

// HTTPClient is an output type that pushes messages to HTTPClient.
type HTTPClient struct {
	client http.Client

	stats metrics.Type
	log   log.Modular

	backoffOn map[int]struct{}
	dropOn    map[int]struct{}

	conf          HTTPClientConfig
	retryThrottle *throttle.Type

	closeChan chan struct{}
}

// NewHTTPClient creates a new HTTPClient writer type.
func NewHTTPClient(conf HTTPClientConfig, log log.Modular, stats metrics.Type) *HTTPClient {
	h := HTTPClient{
		stats:     stats,
		log:       log.NewModule(".output.http"),
		conf:      conf,
		backoffOn: map[int]struct{}{},
		dropOn:    map[int]struct{}{},
		closeChan: make(chan struct{}),
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

	h.retryThrottle = throttle.New(
		throttle.OptMaxUnthrottledRetries(0),
		throttle.OptCloseChan(h.closeChan),
		throttle.OptThrottlePeriod(time.Millisecond*time.Duration(conf.RetryMS)),
		throttle.OptMaxExponentPeriod(time.Millisecond*time.Duration(conf.MaxBackoffMS)),
	)

	return &h
}

//------------------------------------------------------------------------------

// Connect does nothing.
func (h *HTTPClient) Connect() error {
	h.log.Infof("Sending messages via HTTP requests to: %s\n", h.conf.URL)
	return nil
}

// createRequest creates an HTTP request out of a single message.
func (h *HTTPClient) createRequest(msg types.Message) (req *http.Request, err error) {
	if len(msg.GetAll()) == 1 {
		body := bytes.NewBuffer(msg.GetAll()[0])
		if req, err = http.NewRequest(
			h.conf.Verb,
			h.conf.URL,
			body,
		); err == nil {
			req.Header.Add("Content-Type", h.conf.ContentType)
		}
	} else {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len() && err == nil; i++ {
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{h.conf.ContentType},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Get(i)))
			}
		}

		writer.Close()
		if req, err = http.NewRequest(
			h.conf.Verb,
			h.conf.URL,
			body,
		); err == nil {
			req.Header.Add("Content-Type", writer.FormDataContentType())
		}
	}
	err = h.conf.Config.Sign(req)
	return
}

// checkStatus compares a returned status code against configured logic
// determining whether the send is resolved, and if not whether the retry should
// be linear.
func (h *HTTPClient) checkStatus(code int) (resolved bool, linearRetry bool) {
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

// Write attempts to send a message to an HTTP server, this attempt may include
// retries, and if all retries fail an error is returned.
func (h *HTTPClient) Write(msg types.Message) error {
	// POST message
	var req *http.Request
	var res *http.Response
	var err error

	if req, err = h.createRequest(msg); err != nil {
		return err
	}

	rateLimited := false
	if res, err = h.client.Do(req); err == nil {
		if resolved, linear := h.checkStatus(res.StatusCode); !resolved {
			rateLimited = !linear
			err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
		}
		res.Body.Close()
	}

	i, j := 0, h.conf.NumRetries
	for i < j && err != nil {
		req, err = h.createRequest(msg)
		if err != nil {
			continue
		}
		if rateLimited {
			if !h.retryThrottle.ExponentialRetry() {
				return types.ErrTypeClosed
			}
		} else {
			if !h.retryThrottle.Retry() {
				return types.ErrTypeClosed
			}
		}
		rateLimited = false
		if res, err = h.client.Do(req); err == nil {
			if resolved, linear := h.checkStatus(res.StatusCode); !resolved {
				rateLimited = !linear
				err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
			}
			res.Body.Close()
		}
		i++
	}

	if err == nil {
		h.retryThrottle.Reset()
	}

	return err
}

// CloseAsync shuts down the HTTPClient output and stops processing messages.
func (h *HTTPClient) CloseAsync() {
	close(h.closeChan)
}

// WaitForClose blocks until the HTTPClient output has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
