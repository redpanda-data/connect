// Copyright (c) 2019 Ashley Jeffs
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
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/influxdata/go-syslog/rfc5424"
	"github.com/opentracing/opentracing-go"
)

func init() {
	Constructors[TypeSyslog] = TypeSpec{
		constructor: NewSyslog,
		description: `
EXPERIMENTAL: This processor is considered experimental and is therefore subject
to change outside of major version releases.

Parses messages as a properly formed syslog message and then
overwrites the previous contents with the new JSON structure.

### Rfc
#### ` + "`rfc5424`" + `

Parses message which is formed by rfc5424. The resulting keys (if present) would be:
"message", "timestamp", "hostname", "procid", "appname", msgid", "structureddata".

For example, given the syslog message:

` + "```" + `
<42>4 2049-10-11T22:14:15.003Z toaster.smarthome myapp - 2 [home01 device_id="43"] failed to make a toast.
` + "```" + `

The resulting JSON structure would look like this:

` + "```json" + `
{
	"appname": "myapp",
	"hostname": "toaster.smarthome",
	"message": "failed to make a toast.",
	"msgid": "2",
	"structureddata": {
	  "home01": {
		"device_id": "43"
	  }
	},
	"timestamp": "2049-10-11T22:14:15.003Z"
  }
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// SyslogConfig contains configuration fields for the Syslog processor.
type SyslogConfig struct {
	Parts []int  `json:"parts" yaml:"parts"`
	Rfc   string `json:"rfc" yaml:"rfc"`
}

// NewSyslogConfig returns a SyslogConfig with default values.
func NewSyslogConfig() SyslogConfig {
	return SyslogConfig{
		Parts: []int{},
		Rfc:   "rfc5424",
	}
}

//------------------------------------------------------------------------------

// Syslog is a processor that parses properly formed syslog messages.
type Syslog struct {
	parts []int

	conf  Config
	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewSyslog returns a Syslog processor.
func NewSyslog(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if conf.Syslog.Rfc != "rfc5424" {
		return nil, fmt.Errorf("rfc not recognised: %v", conf.Syslog.Rfc)
	}
	s := &Syslog{
		parts: conf.Syslog.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}
	return s, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (s *Syslog) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	s.mCount.Incr(1)
	newMsg := msg.Copy()
	be := true
	proc := func(index int, span opentracing.Span, part types.Part) error {
		p := rfc5424.NewParser()
		res, err := p.Parse(part.Get(), &be)
		if err != nil {
			s.mErr.Incr(1)
			s.log.Debugf("Failed to parse part as syslog-%v: %v\n", s.conf.Syslog.Rfc, err)
			return err
		}
		jObj := map[string]interface{}{}
		if res.Message() != nil {
			jObj["message"] = *res.Message()
		}
		if res.Timestamp() != nil {
			jObj["timestamp"] = *res.Timestamp()
		}
		if res.Hostname() != nil {
			jObj["hostname"] = *res.Hostname()
		}
		if res.ProcID() != nil {
			jObj["procid"] = *res.ProcID()
		}
		if res.Appname() != nil {
			jObj["appname"] = *res.Appname()
		}
		if res.MsgID() != nil {
			jObj["msgid"] = *res.MsgID()
		}
		if res.StructuredData() != nil {
			jObj["structureddata"] = *res.StructuredData()
		}

		part.SetJSON(jObj)
		return nil
	}

	IteratePartsWithSpan(TypeSyslog, s.parts, newMsg, proc)

	s.mBatchSent.Incr(1)
	s.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *Syslog) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *Syslog) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
