package query

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

func TestFunctions(t *testing.T) {
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
	}{
		"literal function": {
			input:    `5`,
			output:   `5`,
			messages: []easyMsg{{}},
		},
		"literal function 2": {
			input:    `"foo"`,
			output:   `foo`,
			messages: []easyMsg{{}},
		},
		"literal function 3": {
			input:    `5 - 2`,
			output:   `3`,
			messages: []easyMsg{{}},
		},
		"literal function 4": {
			input:    `false`,
			output:   `false`,
			messages: []easyMsg{{}},
		},
		"json function": {
			input:  `json()`,
			output: `{"foo":"bar"}`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
				{content: `not json`},
			},
		},
		"json function 2": {
			input:  `json("foo")`,
			output: `bar`,
			messages: []easyMsg{
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 3": {
			input:  `json("foo")`,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json function 4": {
			input:  `json("foo") && (json("bar") > 2)`,
			output: `true`,
			messages: []easyMsg{
				{content: `{"foo":true,"bar":3}`},
			},
		},
		"json function 5": {
			input:  `json("foo") && (json("bar") > 2)`,
			output: `false`,
			messages: []easyMsg{
				{content: `{"foo":true,"bar":1}`},
			},
		},
		"json_from function": {
			input:  `json_from(1, "foo")`,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 2": {
			input:  `json_from(0, "foo")`,
			output: `null`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 3": {
			input:  `json_from(-1, "foo")`,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"hostname": {
			input:  `hostname()`,
			output: fmt.Sprintf(`%v`, hostname),
		},
		"metadata 1": {
			input:  `meta("foo")`,
			output: `bar`,
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
		"metadata 2": {
			input:  `meta("bar")`,
			output: "",
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
			input:  `meta()`,
			output: `{"baz":"qux","duck,1":"quack","foo":"bar"}`,
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
			input:  `meta("duck,1")`,
			output: "quack",
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
			input:  `meta_from(1, "foo")`,
			output: "bar",
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
			input:  `meta("foo")`,
			output: ``,
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
		"metadata 7": {
			input:  `meta_from(1)`,
			output: `{}`,
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
		"metadata 8": {
			input:  `meta_from(1)`,
			output: `{"baz":"qux","duck,1":"quack","foo":"bar"}`,
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
		"error function": {
			input:  `error()`,
			output: `test error`,
			messages: []easyMsg{
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"error function 2": {
			input:  `error_from(1)`,
			output: ``,
			messages: []easyMsg{
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"error function 3": {
			input:  `error_from(1)`,
			output: `test error`,
			messages: []easyMsg{
				{},
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"error function 4": {
			input:  `error()`,
			output: `test error`,
			index:  1,
			messages: []easyMsg{
				{},
				{meta: map[string]string{
					types.FailFlagKey: "test error",
				}},
			},
		},
		"content function": {
			input:  `content()`,
			output: `foobar`,
			index:  0,
			messages: []easyMsg{
				{content: `foobar`},
				{content: `barbaz`},
			},
		},
		"content function 2": {
			input:  `content_from(1)`,
			output: `barbaz`,
			index:  0,
			messages: []easyMsg{
				{content: `foobar`},
				{content: `barbaz`},
			},
		},
		"content function 3": {
			input:  `content()`,
			output: `barbaz`,
			index:  1,
			messages: []easyMsg{
				{content: `foobar`},
				{content: `barbaz`},
			},
		},
		"batch size": {
			input:  `batch_size()`,
			output: `2`,
			messages: []easyMsg{
				{}, {},
			},
		},
		"batch size 2": {
			input:  `batch_size()`,
			output: `1`,
			messages: []easyMsg{
				{},
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

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

			e, err := tryParse(test.input)
			if !assert.NoError(t, err) {
				return
			}
			res := e.ToString(test.index, msg, false)
			assert.Equal(t, test.output, res)
			res = string(e.ToBytes(test.index, msg, false))
			assert.Equal(t, test.output, res)
		})
	}
}

func TestCountersFunction(t *testing.T) {
	tests := [][2]string{
		{`count("foo2")`, "1"},
		{`count("bar2")`, "1"},
		{`count("foo2")`, "2"},
		{`count("foo2")`, "3"},
		{`count("bar2")`, "2"},
		{`count("bar2")`, "3"},
		{`count("foo2")`, "4"},
		{`count("foo2")`, "5"},
		{`count("bar2")`, "4"},
		{`count("bar2")`, "5"},
	}

	for _, test := range tests {
		e, err := tryParse(test[0])
		if !assert.NoError(t, err) {
			continue
		}
		res := e.ToString(0, message.New(nil), false)
		assert.Equal(t, test[1], res)
	}
}

func TestUUIDV4Function(t *testing.T) {
	results := map[string]struct{}{}

	for i := 0; i < 100; i++ {
		e, err := tryParse("uuid_v4()")
		if !assert.NoError(t, err) {
			continue
		}
		res := e.ToString(0, message.New(nil), false)
		if _, exists := results[res]; exists {
			t.Errorf("Duplicate UUID generated: %v", res)
		}
		results[res] = struct{}{}
	}
}

func TestTimestamps(t *testing.T) {
	now := time.Now()

	e, err := tryParse("timestamp_unix_nano()")
	if !assert.NoError(t, err) {
		return
	}
	tStamp := e.ToString(0, message.New(nil), false)

	nanoseconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen := time.Unix(0, nanoseconds)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = tryParse("timestamp_unix()")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.ToString(0, message.New(nil), false)

	seconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(seconds, 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = tryParse("timestamp_unix(10)")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.ToString(0, message.New(nil), false)

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
	e, err = tryParse("timestamp()")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.ToString(0, message.New(nil), false)

	tThen, err = time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", tStamp)
	if err != nil {
		t.Fatal(err)
	}

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, err = tryParse("timestamp_utc()")
	if !assert.NoError(t, err) {
		return
	}
	tStamp = e.ToString(0, message.New(nil), false)

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
}
