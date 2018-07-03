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

package processor

import (
	"fmt"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/client"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["http"] = TypeSpec{
		constructor: NewHTTP,
		description: `
Performs an HTTP request using a message part as the request body and either
replaces or augments the original message part with the body of the response.

By default the entire contents of the message part are sent and the response
entirely replaces the original contents. Alternatively, populating the
` + "`request_map` and `response_map`" + ` fields with a map of destination to
source dot paths allows you to specify how the request payload is constructed,
and how the response is mapped to the original payload respectively.

When ` + "`strict_request_map`" + ` is set to ` + "`true`" + ` the processor is
skipped for any payloads where a map target is not found.

If the list of target parts is empty the processor will be applied to all
message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.`,
	}
}

//------------------------------------------------------------------------------

// HTTPConfig contains any configuration for the HTTP processor.
type HTTPConfig struct {
	Parts            []int             `json:"parts" yaml:"parts"`
	Client           client.Config     `json:"request" yaml:"request"`
	RequestMap       map[string]string `json:"request_map" yaml:"request_map"`
	ResponseMap      map[string]string `json:"response_map" yaml:"response_map"`
	StrictReqMapping bool              `json:"strict_request_map" yaml:"strict_request_map"`
}

// NewHTTPConfig returns a HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Parts:            []int{},
		Client:           client.NewConfig(),
		RequestMap:       map[string]string{},
		ResponseMap:      map[string]string{},
		StrictReqMapping: true,
	}
}

//------------------------------------------------------------------------------

// HTTP is a processor that executes HTTP queries on a message part and
// replaces the contents with the result.
type HTTP struct {
	parts  []int
	client *client.Type

	reqMap map[string]string
	resMap map[string]string

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount       metrics.StatCounter
	mErrJSONS    metrics.StatCounter
	mErrJSONP    metrics.StatCounter
	mErrJSONPReq metrics.StatCounter
	mErrHTTP     metrics.StatCounter
	mErrReq      metrics.StatCounter
	mReqSkipped  metrics.StatCounter
	mErrRes      metrics.StatCounter
	mSucc        metrics.StatCounter
	mSent        metrics.StatCounter
}

// NewHTTP returns a HTTP processor.
func NewHTTP(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	g := &HTTP{
		parts:  conf.HTTP.Parts,
		client: client.New(conf.HTTP.Client),

		reqMap: conf.HTTP.RequestMap,
		resMap: conf.HTTP.ResponseMap,

		conf:  conf,
		log:   log.NewModule(".processor.http"),
		stats: stats,

		mCount:       stats.GetCounter("processor.http.count"),
		mErrHTTP:     stats.GetCounter("processor.http.error.request"),
		mErrJSONS:    stats.GetCounter("processor.http.error.json_set"),
		mErrJSONP:    stats.GetCounter("processor.http.error.json_parse"),
		mErrJSONPReq: stats.GetCounter("processor.http.error.json_parse_request"),
		mErrReq:      stats.GetCounter("processor.http.error.request_map"),
		mReqSkipped:  stats.GetCounter("processor.http.skipped.request_map"),
		mErrRes:      stats.GetCounter("processor.http.error.response_map"),
		mSucc:        stats.GetCounter("processor.http.success"),
		mSent:        stats.GetCounter("processor.http.sent"),
	}
	return g, nil
}

//------------------------------------------------------------------------------

// ProcessMessage parses message parts as grok patterns.
func (h *HTTP) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	h.mCount.Incr(1)

	newMsg := msg.ShallowCopy()

	targetParts := h.parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

partIter:
	for _, index := range targetParts {
		var requestBytes []byte
		if len(h.reqMap) > 0 {
			gReq := gabs.New()
			partJSON, err := msg.GetJSON(index)
			if err == nil {
				var gPayload *gabs.Container
				if gPayload, err = gabs.Consume(partJSON); err == nil {
					for k, v := range h.reqMap {
						if val := gPayload.Path(v).Data(); val != nil {
							gReq.SetP(val, k)
						} else if h.conf.HTTP.StrictReqMapping {
							h.mReqSkipped.Incr(1)
							h.log.Debugf("Request map target not found: %v\n", v)
							continue partIter
						}
					}
				}
			}
			if err != nil {
				h.mErrJSONP.Incr(1)
				h.log.Debugf("Failed to parse body: %v\n", err)
				continue partIter
			}
			requestBytes = gReq.Bytes()
		} else {
			requestBytes = msg.Get(index)
		}

		responseMsg, err := h.client.Send(types.NewMessage([][]byte{requestBytes}))
		if err != nil {
			h.mErrHTTP.Incr(1)
			return nil, types.NewSimpleResponse(fmt.Errorf(
				"HTTP request '%v' failed: %v", h.conf.HTTP.Client.URL, err,
			))
		}

		if responseMsg.Len() < 1 {
			h.mErrHTTP.Incr(1)
			return nil, types.NewSimpleResponse(fmt.Errorf(
				"HTTP response from '%v' was empty", h.conf.HTTP.Client.URL,
			))
		}

		if len(h.resMap) > 0 {
			var gResult, gResponse *gabs.Container

			var partJSON, responseJSON interface{}
			if partJSON, err = msg.GetJSON(index); err == nil {
				if gResult, err = gabs.Consume(partJSON); err == nil {
					if responseJSON, err = responseMsg.GetJSON(0); err == nil {
						gResponse, err = gabs.Consume(responseJSON)
					}
				}
			}
			if err != nil {
				h.mErrJSONPReq.Incr(1)
				h.log.Debugf("Failed to parse response body: %v\n", err)
				continue partIter
			}

			for k, v := range h.resMap {
				if val := gResponse.Path(v).Data(); val != nil {
					gResult.SetP(val, k)
				}
			}

			if err = newMsg.SetJSON(index, gResult.Data()); err != nil {
				h.mErrJSONS.Incr(1)
				h.log.Debugf("Failed to convert result into json: %v\n", err)
				continue partIter
			}
		} else {
			newMsg.Set(index, responseMsg.Get(0))
		}
		h.mSucc.Incr(1)
	}

	msgs := [1]types.Message{newMsg}

	h.mSent.Incr(1)
	return msgs[:], nil
}

//------------------------------------------------------------------------------
