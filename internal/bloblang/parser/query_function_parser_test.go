package parser

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestFunctionQueries(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
		err     error
	}

	tests := map[string]struct {
		input    string
		output   string
		messages []easyMsg
		value    *any
		index    int
	}{
		"without method": {
			input: `this.without("bar","baz")`,
			value: func() *any {
				var v any = map[string]any{
					"foo": 1.0,
					"bar": 2.0,
					"baz": 3.0,
				}
				return &v
			}(),
			output: `{"foo":1}`,
		},
		"without method trailing comma": {
			input: `this.without("bar","baz",)`,
			value: func() *any {
				var v any = map[string]any{
					"foo": 1.0,
					"bar": 2.0,
					"baz": 3.0,
				}
				return &v
			}(),
			output: `{"foo":1}`,
		},
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
		"literal function 5": {
			input:    `null`,
			output:   `null`,
			messages: []easyMsg{{}},
		},
		"literal function 6": {
			input:    `null | "a string"`,
			output:   `a string`,
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
		"json function dynamic arg": {
			input:  `json(meta("path"))`,
			output: `this`,
			messages: []easyMsg{
				{
					content: `{"foo":{"bar":"this"}}`,
					meta: map[string]any{
						"path": "foo.bar",
					},
				},
			},
		},
		"json function dynamic arg 2": {
			input:  `json(json("path"))`,
			output: `this`,
			messages: []easyMsg{
				{content: `{"path":"foo.bar","foo":{"bar":"this"}}`},
			},
		},
		"json function dynamic arg 3": {
			input:  `json().(json(path))`,
			output: `this`,
			messages: []easyMsg{
				{content: `{"path":"foo","foo":"this"}`},
			},
		},
		"json_from function": {
			input:  `json("foo").from(1)`,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"json_from function 3": {
			input:  `json("foo").from(-1)`,
			output: `bar`,
			messages: []easyMsg{
				{content: `not json`},
				{content: `{"foo":"bar"}`},
			},
		},
		"metadata triple quote string arg 1": {
			input:  `meta("""foo""")`,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{},
				{
					meta: map[string]any{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata triple quote string arg 2": {
			input: `meta("""foo
bar""")`,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{},
				{
					meta: map[string]any{
						"foo\nbar": "bar",
						"baz":      "qux",
						"duck,1":   "quack",
					},
				},
			},
		},
		"metadata 1": {
			input:  `meta("foo")`,
			output: `bar`,
			index:  1,
			messages: []easyMsg{
				{},
				{
					meta: map[string]any{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 2": {
			input:  `meta("bar")`,
			output: "null",
			messages: []easyMsg{
				{
					meta: map[string]any{
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
					meta: map[string]any{
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
					meta: map[string]any{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 5": {
			input:  `meta("foo").from(1)`,
			output: "bar",
			index:  0,
			messages: []easyMsg{
				{},
				{
					meta: map[string]any{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 6": {
			input:  `meta("foo")`,
			output: `null`,
			index:  1,
			messages: []easyMsg{
				{
					meta: map[string]any{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
				{},
			},
		},
		"metadata 7": {
			input:  `meta().from(1)`,
			output: `{}`,
			messages: []easyMsg{
				{
					meta: map[string]any{
						"foo":    "bar",
						"baz":    "qux",
						"duck,1": "quack",
					},
				},
			},
		},
		"metadata 8": {
			input:  `meta().from(1)`,
			output: `{"baz":"qux","duck,1":"quack","foo":"bar"}`,
			messages: []easyMsg{
				{},
				{
					meta: map[string]any{
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
				{err: errors.New("test error")},
			},
		},
		"error function 2": {
			input:  `error().from(1)`,
			output: `null`,
			messages: []easyMsg{
				{err: errors.New("test error")},
			},
		},
		"error function 3": {
			input:  `error().from(1)`,
			output: `test error`,
			messages: []easyMsg{
				{},
				{err: errors.New("test error")},
			},
		},
		"error function 4": {
			input:  `error()`,
			output: `test error`,
			index:  1,
			messages: []easyMsg{
				{},
				{err: errors.New("test error")},
			},
		},
		"errored function": {
			input:  `errored()`,
			output: `true`,
			messages: []easyMsg{
				{err: errors.New("test error")},
			},
		},
		"errored function if": {
			input:  `if errored() { "failed" } else { "succeeded" }`,
			output: `failed`,
			index:  1,
			messages: []easyMsg{
				{},
				{err: errors.New("test error")},
			},
		},
		"errored function else": {
			input:  `if errored() { "failed" } else { "succeeded" }`,
			output: `succeeded`,
			index:  0,
			messages: []easyMsg{
				{},
				{err: errors.New("test error")},
			},
		},
		"errored function 2": {
			input:  `errored().from(1)`,
			output: `false`,
			messages: []easyMsg{
				{err: errors.New("test error")},
			},
		},
		"errored function 3": {
			input:  `errored().from(1)`,
			output: `true`,
			messages: []easyMsg{
				{},
				{err: errors.New("test error")},
			},
		},
		"errored function 4": {
			input:  `errored()`,
			output: `true`,
			index:  1,
			messages: []easyMsg{
				{},
				{err: errors.New("test error")},
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
			input:  `content().from(1)`,
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
		"batch index": {
			input:  `batch_index()`,
			output: `1`,
			index:  1,
			messages: []easyMsg{
				{}, {},
			},
		},
		"batch index 2": {
			input:  `batch_index()`,
			output: `0`,
			index:  0,
			messages: []easyMsg{
				{}, {},
			},
		},
		"batch index 3": {
			input:  `batch_index().from(1)`,
			output: `1`,
			index:  0,
			messages: []easyMsg{
				{}, {},
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
		"field root": {
			input:  `this`,
			output: `test`,
			value: func() *any {
				var v any = "test"
				return &v
			}(),
		},
		"field root null": {
			input:  `this`,
			output: `null`,
			value: func() *any {
				var v any
				return &v
			}(),
		},
		"field map": {
			input:  `this.foo`,
			output: `hello world`,
			value: func() *any {
				var v any = map[string]any{
					"foo": "hello world",
				}
				return &v
			}(),
		},
		"field literal": {
			input:  `this.foo.bar`,
			output: `hello world`,
			value: func() *any {
				var v any = map[string]any{
					"foo": map[string]any{
						"bar": "hello world",
					},
				}
				return &v
			}(),
		},
		"field literal 2": {
			input:  `json().map(this.foo.bar)`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"hello world"}}`},
			},
		},
		"field literal 3": {
			input:  `json().map(this.foo.bar)`,
			output: `null`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"hello world"}}`},
			},
		},
		"field literal 4": {
			input:  `json("foo").map(this.bar | this.baz)`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"hello world"}}`},
			},
		},
		"field literal 5": {
			input:  `json().(foo)`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":"hello world"}`},
			},
		},
		"field literal root": {
			input:  `this`,
			output: `test`,
			value: func() *any {
				var v any = "test"
				return &v
			}(),
		},
		"field literal root 2": {
			input:  `this.foo`,
			output: `test`,
			value: func() *any {
				var v any = map[string]any{
					"foo": "test",
				}
				return &v
			}(),
		},
		"field quoted literal": {
			input:  `this."foo.bar"`,
			output: `test`,
			value: func() *any {
				var v any = map[string]any{
					"foo.bar": "test",
				}
				return &v
			}(),
		},
		"field quoted literal extended": {
			input:  `this."foo.bar".baz`,
			output: `test`,
			value: func() *any {
				var v any = map[string]any{
					"foo.bar": map[string]any{
						"baz": "test",
					},
				}
				return &v
			}(),
		},
		"map literal": {
			input:  `json().foo`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":"hello world"}`},
			},
		},
		"map literal 2": {
			input:  `json().foo.bar`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"hello world"}}`},
			},
		},
		"map literal 3": {
			input:  `json().foo.bar`,
			output: `null`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"hello world"}}`},
			},
		},
		"map literal 4": {
			input:  `json("foo").(this.bar | this.baz)`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"hello world"}}`},
			},
		},
		"map literal 5": {
			input:  `json().foo.bar nah`,
			output: `test`,
			messages: []easyMsg{
				{content: `{"foo":{"bar":"test"}}`},
			},
		},
		"map literal 6": {
			input:  `json("foo").(bar | baz)`,
			output: `hello world`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"hello world"}}`},
			},
		},
		"map literal 7": {
			input:  `json("foo").(bar | baz | quz).from_all()`,
			output: `["from_baz","from_quz","from_bar"]`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"from_baz"},"quz":"not this"}`},
				{content: `{"foo":{"quz":"from_quz"}}`},
				{content: `{"foo":{"bar":"from_bar"},"baz":"and not this"}`},
			},
		},
		"map literal 8": {
			input:  `json().foo.(bar | baz | quz).from_all()`,
			output: `["from_baz","from_quz","from_bar"]`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"from_baz"},"quz":"not this"}`},
				{content: `{"foo":{"quz":"from_quz"}}`},
				{content: `{"foo":{"bar":"from_bar"},"baz":"and not this"}`},
			},
		},
		"map literal 9": {
			input:  `json().(foo.bar | foo.baz | foo.quz).from_all()`,
			output: `["from_baz","from_quz","from_bar"]`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"from_baz"},"quz":"not this"}`},
				{content: `{"foo":{"quz":"from_quz"}}`},
				{content: `{"foo":{"bar":"from_bar"},"baz":"and not this"}`},
			},
		},
		"map literal with comments": {
			input: `json(
	"foo" # Here's a thing
).(
	bar | # And look at this thing
	baz |
	quz
).from_all() # And this`,
			output: `["from_baz","from_quz","from_bar"]`,
			messages: []easyMsg{
				{content: `{"foo":{"baz":"from_baz"},"quz":"not this"}`},
				{content: `{"foo":{"quz":"from_quz"}}`},
				{content: `{"foo":{"bar":"from_bar"},"baz":"and not this"}`},
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			msg := message.QuickBatch(nil)
			for _, m := range test.messages {
				part := message.NewPart([]byte(m.content))
				if m.meta != nil {
					for k, v := range m.meta {
						part.MetaSetMut(k, v)
					}
				}
				if m.err != nil {
					part.ErrorSet(m.err)
				}
				msg = append(msg, part)
			}

			e, perr := tryParseQuery(test.input)
			require.Nil(t, perr)

			res, err := query.ExecToString(e, query.FunctionContext{
				Index: test.index, MsgBatch: msg,
			}.WithValueFunc(func() *any { return test.value }))
			require.NoError(t, err)
			assert.Equal(t, test.output, res)

			bres, err := query.ExecToBytes(e, query.FunctionContext{
				Index: test.index, MsgBatch: msg,
			}.WithValueFunc(func() *any { return test.value }))
			require.NoError(t, err)
			res = string(bres)
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
		e, perr := tryParseQuery(test[0])
		require.Nil(t, perr)

		res, err := query.ExecToString(e, query.FunctionContext{
			MsgBatch: message.QuickBatch(nil),
		})
		require.NoError(t, err)
		assert.Equal(t, test[1], res)
	}
}

func TestUUIDV4Function(t *testing.T) {
	results := map[string]struct{}{}

	for i := 0; i < 100; i++ {
		e, perr := tryParseQuery("uuid_v4()")
		require.Nil(t, perr)

		res, err := query.ExecToString(e, query.FunctionContext{
			MsgBatch: message.QuickBatch(nil),
		})
		if _, exists := results[res]; exists {
			t.Errorf("Duplicate UUID generated: %v", res)
		}
		require.NoError(t, err)
		results[res] = struct{}{}
	}
}

func TestTimestamps(t *testing.T) {
	now := time.Now()

	e, perr := tryParseQuery("timestamp_unix_nano()")
	require.Nil(t, perr)

	tStamp, err := query.ExecToString(e, query.FunctionContext{MsgBatch: message.QuickBatch(nil)})
	require.NoError(t, err)

	nanoseconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen := time.Unix(0, nanoseconds)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, perr = tryParseQuery("timestamp_unix()")
	if !assert.Nil(t, perr) {
		require.NoError(t, perr.Err)
	}

	tStamp, err = query.ExecToString(e, query.FunctionContext{MsgBatch: message.QuickBatch(nil)})
	require.NoError(t, err)

	seconds, err := strconv.ParseInt(tStamp, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(seconds, 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}

	now = time.Now()
	e, perr = tryParseQuery("timestamp_unix()")
	if !assert.Nil(t, perr) {
		require.NoError(t, perr.Err)
	}

	tStamp, err = query.ExecToString(e, query.FunctionContext{MsgBatch: message.QuickBatch(nil)})
	require.NoError(t, err)

	var secondsF float64
	secondsF, err = strconv.ParseFloat(tStamp, 64)
	if err != nil {
		t.Fatal(err)
	}
	tThen = time.Unix(int64(secondsF), 0)

	if tThen.Sub(now).Seconds() > 5.0 {
		t.Errorf("Timestamps too far out of sync: %v and %v", tThen, now)
	}
}
