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
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["process_map"] = TypeSpec{
		constructor: NewProcessMap,
		description: `
A processor that extracts and maps fields from the original payload into new
objects, applies a list of processors to the newly constructed objects, and
finally maps the result back into the original payload.

Map paths are arbitrary dot paths, target path hierarchies are constructed if
they do not yet exist. Processing is skipped for message parts where the premap
targets aren't found, for optional premap targets use ` + "`premap_optional`" + `.

If the pre-map is empty then the full payload is sent to the processors. The
post-map should not be left empty, if you intend to replace the full payload
with the result then this processor is redundant. Currently only JSON format is
supported for mapping fields from and back to the original payload.

Maps can reference the root of objects either with an empty string or '.', for
example the maps:

` + "``` yaml" + `
premap:
  .: foo.bar
postmap:
  foo.bar: .
` + "```" + `

Would create a new object where the root is the value of ` + "`foo.bar`" + ` and
would map the full contents of the result back into ` + "`foo.bar`" + `.

This processor is useful for performing processors on subsections of a payload.
For example, you could extract sections of a JSON object in order to construct
a request object for an ` + "`http`" + ` processor, then map the result back
into a field within the original object.

If the number of total message parts resulting from the processing steps does
not match the original count then this processor fails and the messages continue
unchanged. Therefore, you should avoid using batch and filter type processors in
this list.

### Batch Ordering

This processor supports batch messages. When message parts are post-mapped after
processing they will be correctly aligned with the original batch. However, the
ordering of premapped message parts as they are sent through processors are not
guaranteed to match the ordering of the original batch.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			procConfs := make([]interface{}, len(conf.ProcessMap.Processors))
			for i, pConf := range conf.ProcessMap.Processors {
				if procConfs[i], err = SanitiseConfig(pConf); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"parts":           conf.ProcessMap.Parts,
				"premap":          conf.ProcessMap.Premap,
				"premap_optional": conf.ProcessMap.PremapOptional,
				"postmap":         conf.ProcessMap.Postmap,
				"processors":      procConfs,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// ProcessMapConfig is a config struct containing fields for the
// ProcessMap processor.
type ProcessMapConfig struct {
	Parts          []int             `json:"parts" yaml:"parts"`
	Premap         map[string]string `json:"premap" yaml:"premap"`
	PremapOptional map[string]string `json:"premap_optional" yaml:"premap_optional"`
	Postmap        map[string]string `json:"postmap" yaml:"postmap"`
	Processors     []Config          `json:"processors" yaml:"processors"`
}

// NewProcessMapConfig returns a default ProcessMapConfig.
func NewProcessMapConfig() ProcessMapConfig {
	return ProcessMapConfig{
		Parts:          []int{},
		Premap:         map[string]string{},
		PremapOptional: map[string]string{},
		Postmap:        map[string]string{},
		Processors:     []Config{},
	}
}

//------------------------------------------------------------------------------

// ProcessMap is a processor that applies a list of child processors to a
// field extracted from the original payload.
type ProcessMap struct {
	parts          []int
	premap         map[string]string
	premapOptional map[string]string
	postmap        map[string]string
	children       []Type

	log log.Modular

	mCount              metrics.StatCounter
	mSkipped            metrics.StatCounter
	mSkippedMap         metrics.StatCounter
	mErr                metrics.StatCounter
	mErrJSONParse       metrics.StatCounter
	mErrMisaligned      metrics.StatCounter
	mErrMisalignedBatch metrics.StatCounter
	mSent               metrics.StatCounter
	mSentParts          metrics.StatCounter
	mDropped            metrics.StatCounter
}

// NewProcessMap returns a ProcessField processor.
func NewProcessMap(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	nsStats := metrics.Namespaced(stats, "processor.process_map")
	nsLog := log.NewModule(".processor.process_map")

	var children []Type
	for _, pconf := range conf.ProcessMap.Processors {
		proc, err := New(pconf, mgr, nsLog, nsStats)
		if err != nil {
			return nil, err
		}
		children = append(children, proc)
	}

	p := &ProcessMap{
		parts:          conf.ProcessMap.Parts,
		premap:         conf.ProcessMap.Premap,
		premapOptional: conf.ProcessMap.PremapOptional,
		postmap:        conf.ProcessMap.Postmap,
		children:       children,

		log: nsLog,

		mCount:              stats.GetCounter("processor.process_map.count"),
		mSkipped:            stats.GetCounter("processor.process_map.skipped"),
		mSkippedMap:         stats.GetCounter("processor.process_map.skipped.premap_target_missing"),
		mErr:                stats.GetCounter("processor.process_map.error"),
		mErrJSONParse:       stats.GetCounter("processor.process_map.error.json_parse"),
		mErrMisaligned:      stats.GetCounter("processor.process_map.error.misaligned"),
		mErrMisalignedBatch: stats.GetCounter("processor.process_map.error.misaligned_messages"),
		mSent:               stats.GetCounter("processor.process_map.sent"),
		mSentParts:          stats.GetCounter("processor.process_map.parts.sent"),
		mDropped:            stats.GetCounter("processor.process_map.dropped"),
	}

	var err error
	if p.premap, err = validateMap(conf.ProcessMap.Premap); err != nil {
		return nil, fmt.Errorf("premap was not valid: %v", err)
	}
	if p.premapOptional, err = validateMap(conf.ProcessMap.PremapOptional); err != nil {
		return nil, fmt.Errorf("optional premap was not valid: %v", err)
	}
	if p.postmap, err = validateMap(conf.ProcessMap.Postmap); err != nil {
		return nil, fmt.Errorf("postmap was not valid: %v", err)
	}

	if len(p.postmap) == 0 {
		return nil, errors.New("postmap replaces the root of the original payload, this processor is redundant")
	}
	for k := range p.postmap {
		if len(k) == 0 {
			return nil, errors.New("postmap replaces the root of the original payload, this processor is redundant")
		}
	}

	return p, nil
}

//------------------------------------------------------------------------------

func validateMap(m map[string]string) (map[string]string, error) {
	newMap := map[string]string{}
	for k, v := range m {
		if k == "." {
			k = ""
		}
		if v == "." {
			v = ""
		}
		if _, exists := newMap[k]; exists {
			return nil, errors.New("root object mapped twice")
		}
		newMap[k] = v
	}

	targets := []string{}
	for k := range newMap {
		targets = append(targets, k)
	}

	for i, trgt1 := range targets {
		if trgt1 == "" && len(targets) > 1 {
			return nil, errors.New("root map target collides with other targets")
		}
		for j, trgt2 := range targets {
			if j == i {
				continue
			}
			t1Split, t2Split := strings.Split(trgt1, "."), strings.Split(trgt2, ".")
			if len(t1Split) == len(t2Split) {
				// Siblings can't collide
				continue
			}
			if len(t1Split) >= len(t2Split) {
				continue
			}
			matchedSubpaths := true
			for k, t1p := range t1Split {
				if t1p != t2Split[k] {
					matchedSubpaths = false
					break
				}
			}
			if matchedSubpaths {
				return nil, fmt.Errorf("map targets '%v' and '%v' collide", trgt1, trgt2)
			}
		}
	}

	if len(newMap) == 1 {
		if v, exists := newMap[""]; exists {
			if v == "" {
				newMap = map[string]string{}
			}
		}
	}

	return newMap, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies child processors to a mapped subset of payloads and
// maps the result back into the original payload.
func (p *ProcessMap) ProcessMessage(msg types.Message) (msgs []types.Message, res types.Response) {
	p.mCount.Incr(1)

	payload := msg.ShallowCopy()
	resMsgs := [1]types.Message{payload}
	msgs = resMsgs[:]

	var targetObjs map[int]*gabs.Container
	if len(p.parts) > 0 {
		targetObjs = make(map[int]*gabs.Container, len(p.parts))
		for _, i := range p.parts {
			if i < 0 {
				i = payload.Len() + i
			}
			if i < 0 || i >= payload.Len() {
				continue
			}
			targetObjs[i] = nil
		}
	} else {
		targetObjs = make(map[int]*gabs.Container, payload.Len())
		for i := 0; i < payload.Len(); i++ {
			targetObjs[i] = nil
		}
	}

	// Parse original payloads. If the payload is invalid we skip it.
	for i := range targetObjs {
		var err error
		var jObj interface{}
		var gObj *gabs.Container
		if jObj, err = payload.GetJSON(i); err == nil {
			gObj, err = gabs.Consume(jObj)
		}
		if err != nil {
			p.mErrJSONParse.Incr(1)
			p.log.Errorf("Failed to parse message part '%v': %v\n", i, err)
			p.log.Debugf("Message part '%v' contents: %q\n", i, payload.Get(i))
			delete(targetObjs, i)
			continue
		}
		targetObjs[i] = gObj
	}

	// Maps request message parts back into the original alignment.
	reqParts := []int{}
	reqMsg := types.NewMessage(nil)

	// Map the original payloads into premapped message parts.
premapLoop:
	for i, gObj := range targetObjs {
		if len(p.premap) == 0 && len(p.premapOptional) == 0 {
			reqParts = append(reqParts, i)
			reqMsg.Append(payload.Get(i))
			continue
		}

		gReq := gabs.New()
		for k, v := range p.premap {
			gTarget := gObj.Path(v)
			if gTarget.Data() == nil {
				p.mSkipped.Incr(1)
				p.mSkippedMap.Incr(1)
				continue premapLoop
			}
			if len(k) == 0 {
				reqMsg.Append([]byte("{}"))
				reqParts = append(reqParts, i)
				reqMsg.SetJSON(-1, gTarget.Data())
				continue premapLoop
			} else {
				gReq.SetP(gTarget.Data(), k)
			}
		}
		for k, v := range p.premapOptional {
			gTarget := gObj.Path(v)
			if gTarget.Data() == nil {
				continue
			}
			if len(k) == 0 {
				reqMsg.Append([]byte("{}"))
				reqParts = append(reqParts, i)
				reqMsg.SetJSON(-1, gTarget.Data())
				continue premapLoop
			} else {
				gReq.SetP(gTarget.Data(), k)
			}
		}

		reqMsg.Append([]byte("{}"))
		reqParts = append(reqParts, i)
		reqMsg.SetJSON(reqMsg.Len()-1, gReq.Data())
	}

	resultMsgs := []types.Message{reqMsg}
	for i := 0; len(resultMsgs) > 0 && i < len(p.children); i++ {
		var nextResultMsgs []types.Message
		for _, m := range resultMsgs {
			var rMsgs []types.Message
			rMsgs, _ = p.children[i].ProcessMessage(m)
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	resMsg := types.NewMessage(nil)
	for _, rMsg := range resultMsgs {
		for _, part := range rMsg.GetAll() {
			resMsg.Append(part)
		}
	}

	if exp, act := len(reqParts), resMsg.Len(); exp != act {
		p.mSent.Incr(1)
		p.mSentParts.Incr(int64(payload.Len()))
		p.mErr.Incr(1)
		p.mErrMisalignedBatch.Incr(1)
		p.log.Errorf("Misaligned processor result batch. Expected %v messages, received %v\n", exp, act)
		return
	}

	for i, j := range reqParts {
		ogObj := targetObjs[j]

		var err error
		var jObj interface{}
		var gObj *gabs.Container
		if jObj, err = resMsg.GetJSON(i); err == nil {
			gObj, err = gabs.Consume(jObj)
		}
		if err != nil {
			p.mErrJSONParse.Incr(1)
			p.log.Errorf("Failed to parse result part '%v': %v\n", j, err)
			p.log.Debugf("Result part '%v' contents: %q\n", j, resMsg.Get(i))
			continue
		}

		for k, v := range p.postmap {
			gTarget := gObj
			if len(v) > 0 {
				gTarget = gTarget.Path(v)
			}
			if gTarget.Data() != nil {
				ogObj.SetP(gTarget.Data(), k)
			}
		}

		payload.SetJSON(j, ogObj.Data())
	}

	p.mSent.Incr(1)
	p.mSentParts.Incr(int64(payload.Len()))
	return
}

//------------------------------------------------------------------------------
