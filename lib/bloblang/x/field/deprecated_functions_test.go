package field

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
)

func TestDeprecatedFunctionExpressions(t *testing.T) {
	hostname, _ := os.Hostname()

	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		index    int
		escaped  bool
		legacy   bool
	}{
		"hostname": {
			input:  `foo ${!hostname} baz`,
			output: fmt.Sprintf(`foo %v baz`, hostname),
			index:  1,
		},
		"metadata 1": {
			input:  `foo ${!metadata:foo} baz`,
			output: `foo bar baz`,
			index:  1,
			legacy: true,
			messages: []easyMsg{
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
				{},
			},
		},
		"metadata 2": {
			input:  `foo ${!metadata:bar} baz`,
			output: "foo  baz",
			messages: []easyMsg{
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 3": {
			input:  `foo ${!metadata} bar`,
			output: `foo  bar`,
			messages: []easyMsg{
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 4": {
			input:  `foo ${!metadata:duck,1,} baz`,
			output: "foo quack baz",
			messages: []easyMsg{
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 5": {
			input:  `foo ${!metadata:foo,1} baz`,
			output: "foo bar baz",
			index:  0,
			messages: []easyMsg{
				{},
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 6": {
			input:  `foo ${!metadata:foo} baz`,
			output: `foo  baz`,
			index:  1,
			messages: []easyMsg{
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
				{},
			},
		},
		"metadata map": {
			input:  `foo ${!metadata_json_object:1} baz`,
			output: `foo {"baz":"qux","duck,1":"quack","foo":"bar"} baz`,
			index:  0,
			messages: []easyMsg{
				{},
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata map 2": {
			input:  `foo ${!metadata_json_object} baz`,
			output: `foo {} baz`,
			index:  1,
			legacy: true,
			messages: []easyMsg{
				{},
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata map 3": {
			input:  `foo ${!metadata_json_object} baz`,
			output: `foo {"baz":"qux","duck,1":"quack","foo":"bar"} baz`,
			index:  1,
			messages: []easyMsg{
				{},
				{
					meta: map[string]string{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"json combo": {
			input:  `${!json_field:foo} ${!json("foo")}`,
			output: `bar1 bar1`,
			messages: []easyMsg{
				{content: `{"foo":"bar1"}`},
				{content: `{"foo":"bar2"}`},
			},
		},
		"json combo 2": {
			input:  `${!json_field:foo} ${!json("foo")}`,
			output: `bar1 bar2`,
			index:  1,
			legacy: true,
			messages: []easyMsg{
				{content: `{"foo":"bar1"}`},
				{content: `{"foo":"bar2"}`},
			},
		},
		"json combo 3": {
			input:  `${!json_field:foo} ${!json("foo")}`,
			output: `bar2 bar2`,
			index:  1,
			messages: []easyMsg{
				{content: `{"foo":"bar1"}`},
				{content: `{"foo":"bar2"}`},
			},
		},
		"json function": {
			input:  `${!json_field}`,
			output: `{"foo":"bar"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 2": {
			input:  `${!json_field:foo}`,
			output: `bar`,
			index:  1,
			legacy: true,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 3": {
			input:  `${!json_field:foo,1}`,
			output: `bar`,
			index:  0,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 4": {
			input:   `${!json_field:foo,0}`,
			output:  `{\"bar\":\"baz\"}`,
			index:   1,
			escaped: true,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"baz"}}`},
			},
		},
		"json function 5": {
			input:  `${!json_field:foo}`,
			output: `null`,
			index:  1,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 6": {
			input:  `${!json_field:foo,0}`,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"error function": {
			input:  `foo ${!error} bar`,
			output: `foo test error bar`,
			messages: []easyMsg{
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"error function 2": {
			input:  `foo ${!error:1} bar`,
			output: `foo  bar`,
			messages: []easyMsg{
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"error function 3": {
			input:  `foo ${!error:1} bar`,
			output: `foo test error bar`,
			messages: []easyMsg{
				{},
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"content function": {
			input:  `hello ${!content} world`,
			output: `hello foobar world`,
			index:  1,
			legacy: true,
			messages: []easyMsg{
				{content: `foobar`},
				{content: `barbaz`},
			},
		},
		"content function 2": {
			input:  `hello ${!content:1} world`,
			output: `hello barbaz world`,
			index:  1,
			messages: []easyMsg{
				{content: `foobar`},
				{content: `barbaz`},
			},
		},
		"content function 3": {
			input:  `hello ${!content} world`,
			output: `hello barbaz world`,
			index:  1,
			messages: []easyMsg{
				{content: `foobar`},
				{content: `barbaz`},
			},
		},
		"batch size": {
			input:  `${!batch_size}`,
			output: `2`,
			messages: []easyMsg{
				{}, {},
			},
		},
		"batch size 2": {
			input:  `${!batch_size}`,
			output: `1`,
			messages: []easyMsg{
				{},
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			msg := message.New(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.Metadata().Set(k, v)
					}
				}
				msg.Append(part)
			}

			e, err := parse(test.input)
			if !assert.NoError(t, err) {
				return
			}
			var res string
			if test.escaped {
				if test.legacy {
					res = string(e.BytesEscapedLegacy(test.index, msg))
				} else {
					res = string(e.BytesEscaped(test.index, msg))
				}
			} else {
				if test.legacy {
					res = e.StringLegacy(test.index, msg)
				} else {
					res = e.String(test.index, msg)
				}
			}
			assert.Equal(t, test.output, res)
		})
	}
}

func TestCountersFunction(t *testing.T) {
	tests := [][2]string{
		{"foo1: ${!count:foo}", "foo1: 1"},
		{"bar1: ${!count:bar}", "bar1: 1"},
		{"foo2: ${!count:foo} ${!count:foo}", "foo2: 2 3"},
		{"bar2: ${!count:bar} ${!count:bar}", "bar2: 2 3"},
		{"foo3: ${!count:foo} ${!count:foo}", "foo3: 4 5"},
		{"bar3: ${!count:bar} ${!count:bar}", "bar3: 4 5"},
	}

	for _, test := range tests {
		e, err := parse(test[0])
		if !assert.NoError(t, err) {
			continue
		}
		res := e.String(0, message.New(nil))
		assert.Equal(t, test[1], res)
	}
}

func TestUUIDV4Function(t *testing.T) {
	results := map[string]struct{}{}

	for i := 0; i < 100; i++ {
		e, err := parse("${!uuid_v4}")
		if !assert.NoError(t, err) {
			continue
		}
		res := e.String(0, message.New(nil))
		if _, exists := results[res]; exists {
			t.Errorf("Duplicate UUID generated: %v", res)
		}
		results[res] = struct{}{}
	}
}

func TestTimestamps(t *testing.T) {
	now := time.Now()

	e, err := parse("${!timestamp_unix_nano}")
	if !assert.NoError(t, err) {
		return
	}
	tStamp := e.String(0, message.New(nil))

	nanoseconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen := time.Unix(0, nanoseconds)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = parse("${!timestamp_unix}")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.String(0, message.New(nil))

	seconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(seconds, 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = parse("${!timestamp_unix:10}")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.String(0, message.New(nil))

	var secondsF float64
	secondsF, err = strconv.ParseFloat(tStamp, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(int64(secondsF), 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = parse("${!timestamp}")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.String(0, message.New(nil))

	tThen, err = time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = parse("${!timestamp_utc}")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.String(0, message.New(nil))

	tThen, err = time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
	if !strings.Contains(tStamp, "UTC") {
		t.Errorf("Non-UTC timezone detected: %v", tStamp)
	}

	now = time.Now()
	e, err = parse("${!timestamp_utc:2006-01-02T15:04:05.000Z}")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.String(0, message.New(nil))

	tThen, err = time.Parse("2006-01-02T15:04:05.000Z", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
}
