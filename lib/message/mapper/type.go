package mapper

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/gabs"
)

//------------------------------------------------------------------------------

// Type contains conditions and maps for transforming a batch of messages into
// a subset of request messages, and mapping results from those requests back
// into the original message batch.
type Type struct {
	log   log.Modular
	stats metrics.Type

	reqMap    map[string]string
	reqOptMap map[string]string

	resMap    map[string]string
	resOptMap map[string]string

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

	if err := validateMap(t.reqMap); err != nil {
		return nil, fmt.Errorf("bad request mandatory map: %v", err)
	}
	if err := validateMap(t.resMap); err != nil {
		return nil, fmt.Errorf("bad response mandatory map: %v", err)
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

func validateMap(m map[string]string) error {
	targets := []string{}
	for k := range m {
		targets = append(targets, k)
	}
	for i, trgt1 := range targets {
		if trgt1 == "." {
			trgt1 = ""
		}
		if trgt1 == "" && len(targets) > 1 {
			return errors.New("root map target collides with other targets")
		}
		for j, trgt2 := range targets {
			if trgt2 == "." {
				trgt2 = ""
			}
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
				return fmt.Errorf("map targets '%v' and '%v' collide", trgt1, trgt2)
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// TargetsUsed returns a list of dot paths that this mapper depends on.
func (t *Type) TargetsUsed() []string {
	depsMap := map[string]struct{}{}
	for _, v := range t.reqMap {
		if len(v) > 0 && v != "." {
			depsMap[v] = struct{}{}
		}
	}
	for _, v := range t.reqOptMap {
		if len(v) > 0 && v != "." {
			depsMap[v] = struct{}{}
		}
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
		if len(k) > 0 && k != "." {
			targetsMap[k] = struct{}{}
		}
	}
	for k := range t.resOptMap {
		if len(k) > 0 && k != "." {
			targetsMap[k] = struct{}{}
		}
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
	payloadObj, err := msg.GetJSON(index)
	if err != nil {
		return nil, err
	}
	var container *gabs.Container
	if container, err = gabs.Consume(payloadObj); err != nil {
		return nil, err
	}
	return container, nil
}

// MapRequests takes a single payload (of potentially multiple parts, where
// parts can potentially be nil) and attempts to create a new payload of mapped
// messages. Also returns an array of message part indexes that were skipped due
// to either failed conditions or being empty.
func (t *Type) MapRequests(payload types.Message) (types.Message, []int, error) {
	mappedMsg := message.New(nil)
	skipped := []int{}

	msg := payload.ShallowCopy()

partLoop:
	for i := 0; i < msg.Len(); i++ {
		// Skip if message part is empty.
		if p := msg.Get(i); p == nil || len(p) == 0 {
			skipped = append(skipped, i)
			continue partLoop
		}

		// Skip if message part fails condition.
		if !t.test(message.Lock(msg, i)) {
			skipped = append(skipped, i)
			continue partLoop
		}

		t.log.Tracef("Unmapped message part '%v': %q\n", i, msg.Get(i))
		sourceObj, err := getGabs(msg, i)
		if err != nil {
			t.mReqErr.Incr(1)
			t.mReqErrJSON.Incr(1)
			t.log.Debugf("Failed to parse message part '%v': %v. Failed part: %q\n", i, err, msg.Get(i))

			// Skip if message part fails JSON parse.
			skipped = append(skipped, i)
			continue partLoop
		}

		destObj := gabs.New()
		if len(t.reqMap) == 0 && len(t.reqOptMap) == 0 {
			destObj = sourceObj
		}
		for k, v := range t.reqMap {
			src := sourceObj
			if len(v) > 0 && v != "." {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					t.mReqErr.Incr(1)
					t.mReqErrMap.Incr(1)
					t.log.Debugf("Failed to find request map target '%v' in message part '%v'. Message contents: %q\n", v, i, msg.Get(i))

					// Skip if message part fails mapping.
					skipped = append(skipped, i)
					continue partLoop
				}
			}
			if len(k) > 0 && k != "." {
				destObj.SetP(src.Data(), k)
			} else {
				destObj = src
			}
		}
		for k, v := range t.reqOptMap {
			src := sourceObj
			if len(v) > 0 && v != "." {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					continue
				}
			}
			if len(k) > 0 && k != "." {
				destObj.SetP(src.Data(), k)
			} else {
				destObj = src
			}
		}

		mappedMsg.Append([]byte("{}"))
		if err = mappedMsg.SetJSON(-1, destObj.Data()); err != nil {
			t.mReqErr.Incr(1)
			t.mReqErrJSON.Incr(1)
			t.log.Debugf("Failed to marshal request map result in message part '%v'. Map contents: '%v'\n", i, destObj.String())
		}
		t.log.Tracef("Mapped request part '%v': %q\n", i, mappedMsg.Get(-1))
	}

	return mappedMsg, skipped, nil
}

// MapResponses attempts to merge a batch of responses with original payloads as
// per the response map.
//
// The count of parts within the response message must match the original
// payload. If parts were removed from the enrichment request the original
// contents must be interlaced back within the response object before calling
// the overlay.
func (t *Type) MapResponses(payload, response types.Message) error {
	if exp, act := payload.Len(), response.Len(); exp != act {
		t.mResErr.Incr(1)
		t.mResErrParts.Incr(1)
		return fmt.Errorf("payload message counts have diverged from the request and response: %v != %v", act, exp)
	}

partLoop:
	for i := 0; i < response.Len(); i++ {
		if response.Get(i) == nil {
			// Parts that are nil are skipped.
			continue partLoop
		}
		t.log.Tracef("Premapped response part '%v': %q\n", i, response.Get(i))

		sourceObj, err := getGabs(response, i)
		if err != nil {
			t.mResErr.Incr(1)
			t.mResErrJSON.Incr(1)
			t.log.Debugf("Failed to parse response part '%v': %v. Failed part: '%s'\n", i, err, response.Get(i))

			// Skip parts that fail JSON parse.
			continue partLoop
		}

		var destObj *gabs.Container
		if destObj, err = getGabs(payload, i); err != nil {
			t.mResErr.Incr(1)
			t.mResErrJSON.Incr(1)
			t.log.Debugf("Failed to parse payload part '%v': %v. Failed part: '%s'\n", i, err, response.Get(i))

			// Skip parts that fail JSON parse.
			continue partLoop
		}

		if len(t.resMap) == 0 && len(t.resOptMap) == 0 {
			destObj = sourceObj
		}
		for k, v := range t.resMap {
			src := sourceObj
			if len(v) > 0 && v != "." {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					t.mResErr.Incr(1)
					t.mResErrMap.Incr(1)
					t.log.Debugf("Failed to find map target '%v' in response part '%v'. Response contents: %q\n", v, i, response.Get(i))

					// Skip parts that fail mapping.
					continue partLoop
				}
			}
			if len(k) > 0 && k != "." {
				destObj.SetP(src.Data(), k)
			} else {
				destObj = src
			}
		}
		for k, v := range t.resOptMap {
			src := sourceObj
			if len(v) > 0 && v != "." {
				src = sourceObj.Path(v)
				if src.Data() == nil {
					continue
				}
			}
			if len(k) > 0 && k != "." {
				destObj.SetP(src.Data(), k)
			} else {
				destObj = src
			}
		}

		if err = payload.SetJSON(i, destObj.Data()); err != nil {
			t.mResErr.Incr(1)
			t.mResErrJSON.Incr(1)
			t.log.Debugf("Failed to marshal response map result in message part '%v'. Map contents: '%v'\n", i, destObj.String())

			// Skip parts that fail mapping.
			continue partLoop
		}

		t.log.Tracef("Mapped message part '%v': %q\n", i, payload.Get(i))
	}

	return nil
}

//------------------------------------------------------------------------------
