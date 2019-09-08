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

package mapper

import (
	"errors"
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

// Type contains conditions and maps for transforming a batch of messages into
// a subset of request messages, and mapping results from those requests back
// into the original message batch.
type Type struct {
	log   log.Modular
	stats metrics.Type

	reqTargets    []string
	reqMap        map[string]string
	reqOptTargets []string
	reqOptMap     map[string]string

	resTargets    []string
	resMap        map[string]string
	resOptTargets []string
	resOptMap     map[string]string

	conditions []types.Condition

	// Metrics
	mErrParts metrics.StatCounter

	mCondPass metrics.StatCounter
	mCondFail metrics.StatCounter

	mReqErr     metrics.StatCounter
	mReqErrJSON metrics.StatCounter
	mReqErrMap  metrics.StatCounter

	mResErr      metrics.StatCounter
	mResErrJSON  metrics.StatCounter
	mResErrParts metrics.StatCounter
	mResErrMap   metrics.StatCounter
}

// New creates a new mapper Type.
func New(opts ...func(*Type)) (*Type, error) {
	t := &Type{
		reqMap:     map[string]string{},
		reqOptMap:  map[string]string{},
		resMap:     map[string]string{},
		resOptMap:  map[string]string{},
		conditions: nil,
		log:        log.Noop(),
		stats:      metrics.Noop(),
	}

	for _, opt := range opts {
		opt(t)
	}

	var err error
	if t.reqTargets, err = validateMap(t.reqMap); err != nil {
		return nil, fmt.Errorf("bad request mandatory map: %v", err)
	}
	if t.reqOptTargets, err = validateMap(t.reqOptMap); err != nil {
		return nil, fmt.Errorf("bad request optional map: %v", err)
	}
	if t.resTargets, err = validateMap(t.resMap); err != nil {
		return nil, fmt.Errorf("bad response mandatory map: %v", err)
	}
	if t.resOptTargets, err = validateMap(t.resOptMap); err != nil {
		return nil, fmt.Errorf("bad response optional map: %v", err)
	}

	t.mErrParts = t.stats.GetCounter("error.parts_diverged")

	t.mCondPass = t.stats.GetCounter("condition.pass")
	t.mCondFail = t.stats.GetCounter("condition.fail")

	t.mReqErr = t.stats.GetCounter("request.error")
	t.mReqErrJSON = t.stats.GetCounter("request.error.json")
	t.mReqErrMap = t.stats.GetCounter("request.error.map")

	t.mResErr = t.stats.GetCounter("response.error")
	t.mResErrParts = t.stats.GetCounter("response.error.parts_diverged")
	t.mResErrMap = t.stats.GetCounter("response.error.map")
	t.mResErrJSON = t.stats.GetCounter("response.error.json")

	return t, nil
}

//------------------------------------------------------------------------------

// OptSetReqMap sets the mandatory request map used by this type.
func OptSetReqMap(m map[string]string) func(*Type) {
	return func(t *Type) {
		t.reqMap = m
	}
}

// OptSetOptReqMap sets the optional request map used by this type.
func OptSetOptReqMap(m map[string]string) func(*Type) {
	return func(t *Type) {
		t.reqOptMap = m
	}
}

// OptSetResMap sets the mandatory response map used by this type.
func OptSetResMap(m map[string]string) func(*Type) {
	return func(t *Type) {
		t.resMap = m
	}
}

// OptSetOptResMap sets the optional response map used by this type.
func OptSetOptResMap(m map[string]string) func(*Type) {
	return func(t *Type) {
		t.resOptMap = m
	}
}

// OptSetConditions sets the conditions used by this type.
func OptSetConditions(conditions []types.Condition) func(*Type) {
	return func(t *Type) {
		t.conditions = conditions
	}
}

// OptSetLogger sets the logger used by this type.
func OptSetLogger(l log.Modular) func(*Type) {
	return func(t *Type) {
		t.log = l
	}
}

// OptSetStats sets the metrics aggregator used by this type.
func OptSetStats(s metrics.Type) func(*Type) {
	return func(t *Type) {
		t.stats = s
	}
}

//------------------------------------------------------------------------------

func validateMap(m map[string]string) ([]string, error) {
	targets := make([]string, 0, len(m))
	for k, v := range m {
		if k == "." {
			if _, exists := m[""]; exists {
				return nil, errors.New("dot path '.' and empty path '' both set root")
			}
			m[""] = v
			delete(m, ".")
			k = ""
		}
		if v == "." {
			m[k] = ""
		}
		targets = append(targets, k)
	}
	sort.Slice(targets, func(i, j int) bool { return len(targets[i]) < len(targets[j]) })
	return targets, nil
}

//------------------------------------------------------------------------------

// TargetsUsed returns a list of dot paths that this mapper depends on.
func (t *Type) TargetsUsed() []string {
	depsMap := map[string]struct{}{}
	for _, v := range t.reqMap {
		depsMap[v] = struct{}{}
	}
	for _, v := range t.reqOptMap {
		depsMap[v] = struct{}{}
	}
	deps := []string{}
	for k := range depsMap {
		deps = append(deps, k)
	}
	sort.Strings(deps)
	return deps
}

// TargetsProvided returns a list of dot paths that this mapper provides.
func (t *Type) TargetsProvided() []string {
	targetsMap := map[string]struct{}{}
	for k := range t.resMap {
		targetsMap[k] = struct{}{}
	}
	for k := range t.resOptMap {
		targetsMap[k] = struct{}{}
	}
	targets := []string{}
	for k := range targetsMap {
		targets = append(targets, k)
	}
	sort.Strings(targets)
	return targets
}

//------------------------------------------------------------------------------

// test a message against the conditions of a mapper.
func (t *Type) test(msg types.Message) bool {
	for _, c := range t.conditions {
		if !c.Check(msg) {
			t.mCondFail.Incr(1)
			return false
		}
	}
	t.mCondPass.Incr(1)
	return true
}

func getGabs(msg types.Message, index int) (*gabs.Container, error) {
	payloadObj, err := msg.Get(index).JSON()
	if err != nil {
		return nil, err
	}
	container := gabs.Wrap(payloadObj)
	return container, nil
}

// MapRequests takes a single payload (of potentially multiple parts, where
// parts can potentially be nil) and maps the parts according to the request
// mapping.
//
// Two arrays are also returned, the first containing all message part indexes
// that were skipped due to either failed conditions or for being empty. The
// second contains only message part indexes that failed their map stage.
func (t *Type) MapRequests(msg types.Message) (skipped, failed []int) {
	var mappedParts []types.Part

partLoop:
	for i := 0; i < msg.Len(); i++ {
		// Skip if message part is empty.
		if msg.Get(i).IsEmpty() {
			skipped = append(skipped, i)
			continue partLoop
		}

		// Skip if message part fails condition.
		if !t.test(message.Lock(msg, i)) {
			skipped = append(skipped, i)
			continue partLoop
		}

		if len(t.reqMap) == 0 && len(t.reqOptMap) == 0 {
			mappedParts = append(mappedParts, msg.Get(i))
			continue partLoop
		}

		sourceObj, err := getGabs(msg, i)
		if err != nil {
			t.mReqErr.Incr(1)
			t.mReqErrJSON.Incr(1)
			t.log.Debugf("Failed to parse message part '%v': %v. Failed part: %q\n", i, err, msg.Get(i).Get())

			// Skip if message part fails JSON parse.
			failed = append(failed, i)
			continue partLoop
		}

		destObj := gabs.New()
		for _, k := range t.reqTargets {
			v := t.reqMap[k]
			src := sourceObj
			if len(v) > 0 {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					t.mReqErr.Incr(1)
					t.mReqErrMap.Incr(1)
					t.log.Debugf("Failed to find request map target '%v' in message part '%v'.\n", v, i)

					// Skip if message part fails mapping.
					failed = append(failed, i)
					continue partLoop
				}
			}
			srcData, _ := message.CopyJSON(src.Data())
			if len(k) > 0 {
				destObj.SetP(srcData, k)
			} else {
				destObj = gabs.Wrap(srcData)
			}
		}
		for _, k := range t.reqOptTargets {
			v := t.reqOptMap[k]
			src := sourceObj
			if len(v) > 0 {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					continue
				}
			}
			srcData, _ := message.CopyJSON(src.Data())
			if len(k) > 0 {
				destObj.SetP(srcData, k)
			} else {
				destObj = gabs.Wrap(srcData)
			}
		}

		if err = msg.Get(i).SetJSON(destObj.Data()); err != nil {
			t.mReqErr.Incr(1)
			t.mReqErrJSON.Incr(1)
			t.log.Errorf("Failed to marshal request map result in message part '%v'. Map contents: '%v'\n", i, destObj.String())
			failed = append(failed, i)
		} else {
			mappedParts = append(mappedParts, msg.Get(i))
		}
	}

	msg.SetAll(mappedParts)
	return
}

// AlignResult takes the original length of a mapped payload, a slice of skipped
// message part indexes, a slice of failed message part indexes, and a
// post-mapped, post-processed slice of resulting messages, and attempts to
// create a new payload where the results are realigned and ready to map back
// into the original.
func (t *Type) AlignResult(length int, skipped, failed []int, result []types.Message) (types.Message, error) {
	resMsgParts := []types.Part{}
	for _, m := range result {
		m.Iter(func(i int, p types.Part) error {
			resMsgParts = append(resMsgParts, p)
			return nil
		})
	}

	skippedOrFailed := make([]int, len(skipped)+len(failed))
	i := copy(skippedOrFailed, skipped)
	copy(skippedOrFailed[i:], failed)

	sort.Ints(skippedOrFailed)

	// Check that size of response is aligned with payload.
	if rLen, pLen := len(resMsgParts)+len(skippedOrFailed), length; rLen != pLen {
		return nil, fmt.Errorf("parts returned from enrichment do not match payload: %v != %v", rLen, pLen)
	}

	var responseParts []types.Part
	if len(skippedOrFailed) == 0 {
		responseParts = resMsgParts
	} else {
		// Remember to insert nil for each skipped part at the correct index.
		responseParts = make([]types.Part, length)
		sIndex := 0
		for i = 0; i < len(resMsgParts); i++ {
			for sIndex < len(skippedOrFailed) && skippedOrFailed[sIndex] == (i+sIndex) {
				sIndex++
			}
			responseParts[i+sIndex] = resMsgParts[i]
		}
	}

	newMsg := message.New(nil)
	newMsg.SetAll(responseParts)
	return newMsg, nil
}

// MapResponses attempts to merge a batch of responses with original payloads as
// per the response map.
//
// The count of parts within the response message must match the original
// payload. If parts were removed from the enrichment request the original
// contents must be interlaced back within the response object before calling
// the overlay.
//
// Returns an array of message indexes that failed their map stage, or an error.
func (t *Type) MapResponses(payload, response types.Message) ([]int, error) {
	if exp, act := payload.Len(), response.Len(); exp != act {
		t.mResErr.Incr(1)
		t.mResErrParts.Incr(1)
		return nil, fmt.Errorf("payload message counts have diverged from the request and response: %v != %v", act, exp)
	}

	var failed []int

	parts := make([]types.Part, payload.Len())
	payload.Iter(func(i int, p types.Part) error {
		parts[i] = p
		return nil
	})

partLoop:
	for i := 0; i < response.Len(); i++ {
		if response.Get(i).IsEmpty() {
			// Parts that are nil are skipped.
			continue partLoop
		}

		if len(t.resMap) == 0 && len(t.resOptMap) == 0 {
			newPart := message.MetaPartCopy(parts[i])
			newPart.Set(response.Get(i).Get())

			// Overwrite payload parts with new parts metadata.
			metadata := newPart.Metadata()
			response.Get(i).Metadata().Iter(func(k, v string) error {
				metadata.Set(k, v)
				return nil
			})

			parts[i] = newPart
			continue partLoop
		}

		sourceObj, err := getGabs(response, i)
		if err != nil {
			t.mResErr.Incr(1)
			t.mResErrJSON.Incr(1)
			t.log.Debugf("Failed to parse response part '%v': %v. Failed part: '%s'\n", i, err, response.Get(i).Get())

			// Skip parts that fail JSON parse.
			failed = append(failed, i)
			continue partLoop
		}

		// Check all mandatory map targets before proceeding.
		for _, k := range t.resTargets {
			if v := t.resMap[k]; len(v) > 0 && sourceObj.Path(v).Data() == nil {
				t.mResErr.Incr(1)
				t.mResErrMap.Incr(1)
				t.log.Debugf("Failed to find map target '%v' in response part '%v'.\n", v, i)

				// Skip parts that fail mapping.
				failed = append(failed, i)
				continue partLoop
			}
		}

		var destObj *gabs.Container
		if destObj, err = getGabs(payload, i); err != nil {
			t.mResErr.Incr(1)
			t.mResErrJSON.Incr(1)
			t.log.Debugf("Failed to parse payload part '%v': %v. Failed part: '%s'\n", i, err, response.Get(i).Get())

			// Skip parts that fail JSON parse.
			failed = append(failed, i)
			continue partLoop
		}

		for _, k := range t.resTargets {
			v := t.resMap[k]
			src := sourceObj
			if len(v) > 0 {
				src = sourceObj.Path(v)
			}
			if len(k) > 0 {
				destObj.SetP(src.Data(), k)
			} else {
				destObj = src
			}
		}
		for _, k := range t.resOptTargets {
			v := t.resOptMap[k]
			src := sourceObj
			if len(v) > 0 {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					continue
				}
			}
			if len(k) > 0 {
				destObj.SetP(src.Data(), k)
			} else {
				destObj = src
			}
		}

		if err = parts[i].SetJSON(destObj.Data()); err != nil {
			t.mResErr.Incr(1)
			t.mResErrJSON.Incr(1)
			t.log.Debugf("Failed to marshal response map result in message part '%v'. Map contents: '%v'\n", i, destObj.String())

			// Skip parts that fail mapping.
			failed = append(failed, i)
			continue partLoop
		}

		metadata := parts[i].Metadata()
		response.Get(i).Metadata().Iter(func(k, v string) error {
			metadata.Set(k, v)
			return nil
		})
	}

	payload.SetAll(parts)
	return failed, nil
}

//------------------------------------------------------------------------------
