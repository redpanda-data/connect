package query

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
)

var linebreakStr = `foo
bar
baz`

func TestMethods(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]any
	}

	literalFn := func(val any) Function {
		fn := NewLiteralFunction("", val)
		return fn
	}
	jsonFn := func(json string) Function {
		t.Helper()
		gObj, err := gabs.ParseJSON([]byte(json))
		require.NoError(t, err)
		fn := NewLiteralFunction("", gObj.Data())
		return fn
	}
	function := func(name string, args ...any) Function {
		t.Helper()
		fn, err := InitFunctionHelper(name, args...)
		require.NoError(t, err)
		return fn
	}
	arithmetic := func(left, right Function, op ArithmeticOperator) Function {
		t.Helper()
		fn, err := NewArithmeticExpression(
			[]Function{left, right},
			[]ArithmeticOperator{op},
		)
		require.NoError(t, err)
		return fn
	}
	jn := func(i int) json.Number {
		return json.Number(strconv.Itoa(i))
	}

	type easyMethod struct {
		name string
		args []any
	}
	methods := func(fn Function, methods ...easyMethod) Function {
		t.Helper()
		for _, m := range methods {
			var err error
			fn, err = InitMethodHelper(m.name, fn, m.args...)
			require.NoError(t, err)
		}
		return fn
	}
	method := func(name string, args ...any) easyMethod {
		return easyMethod{name: name, args: args}
	}

	tests := map[string]struct {
		input    Function
		value    *any
		output   any
		err      string
		messages []easyMsg
		index    int
	}{
		"check format_json with default indentation": {
			input: methods(
				jsonFn(`{"doc":{"foo":"bar"}}`),
				method("format_json"),
			),
			output: []byte(`{
    "doc": {
        "foo": "bar"
    }
}`),
		},
		"check format_json with two spaces indentation": {
			input: methods(
				jsonFn(`{"doc":{"foo":"bar"}}`),
				method("format_json", "  "),
			),
			output: []byte(`{
  "doc": {
    "foo": "bar"
  }
}`),
		},
		"check format_json with one tab indentation": {
			input: methods(
				jsonFn(`{"doc":{"foo":"bar"}}`),
				method("format_json", "\t"),
			),
			output: []byte(`{
	"doc": {
		"foo": "bar"
	}
}`),
		},
		"check format_json with empty indentation": {
			input: methods(
				jsonFn(`{"doc":{"foo":"bar"}}`),
				method("format_json", ""),
			),
			output: []byte(`{
"doc": {
"foo": "bar"
}
}`),
		},
		"check format_yaml": {
			input: methods(
				jsonFn(`{"doc":{"foo":"bar"}}`),
				method("format_yaml"),
			),
			output: []byte(`doc:
    foo: bar
`),
		},
		"check parse csv 1": {
			input: methods(
				literalFn("foo,bar,baz\n1,2,3\n4,5,6"),
				method("parse_csv"),
			),
			output: []any{
				map[string]any{
					"foo": "1",
					"bar": "2",
					"baz": "3",
				},
				map[string]any{
					"foo": "4",
					"bar": "5",
					"baz": "6",
				},
			},
		},
		"check parse csv 2": {
			input: methods(
				literalFn("foo,bar,baz"),
				method("parse_csv"),
			),
			output: []any{},
		},
		"check parse csv 3": {
			input: methods(
				literalFn("foo,bar\nfoo 1,bar 1\nfoo 2,bar 2"),
				method("parse_csv"),
				method("string"),
			),
			output: `[{"bar":"bar 1","foo":"foo 1"},{"bar":"bar 2","foo":"foo 2"}]`,
		},
		"check parse csv error 1": {
			input: methods(
				literalFn("foo,bar,baz\n1,2,3,4"),
				method("parse_csv"),
			),
			err: "string literal: record on line 2: wrong number of fields",
		},
		"check explode 1": {
			input: methods(
				jsonFn(`{"foo":[1,2,3],"id":"bar"}`),
				method("explode", "foo"),
				method("string"),
			),
			output: `[{"foo":1,"id":"bar"},{"foo":2,"id":"bar"},{"foo":3,"id":"bar"}]`,
		},
		"check explode 2": {
			input: methods(
				jsonFn(`{"foo":{"also":"this","bar":[{"key":"value1"},{"key":"value2"},{"key":"value3"}]},"id":"baz"}`),
				method("explode", "foo.bar"),
				method("string"),
			),
			output: `[{"foo":{"also":"this","bar":{"key":"value1"}},"id":"baz"},{"foo":{"also":"this","bar":{"key":"value2"}},"id":"baz"},{"foo":{"also":"this","bar":{"key":"value3"}},"id":"baz"}]`,
		},
		"check explode 3": {
			input: methods(
				jsonFn(`{"foo":{"a":1,"b":2,"c":3},"id":"bar"}`),
				method("explode", "foo"),
				method("string"),
			),
			output: `{"a":{"foo":1,"id":"bar"},"b":{"foo":2,"id":"bar"},"c":{"foo":3,"id":"bar"}}`,
		},
		"check explode 4": {
			input: methods(
				jsonFn(`{"foo":{"also":"this","bar":{"key1":["a","b"],"key2":{"c":3,"d":4}}},"id":"baz"}`),
				method("explode", "foo.bar"),
				method("string"),
			),
			output: `{"key1":{"foo":{"also":"this","bar":["a","b"]},"id":"baz"},"key2":{"foo":{"also":"this","bar":{"c":3,"d":4}},"id":"baz"}}`,
		},
		"check explode error 1": {
			input: methods(
				jsonFn(`{"foo":"not an array","id":"bar"}`),
				method("explode", "foo"),
				method("string"),
			),
			err: "object literal: expected array or object value at path 'foo', found: string",
		},
		"check explode error 2": {
			input: methods(
				jsonFn(`{"id":"bar"}`),
				method("explode", "foo"),
				method("string"),
			),
			err: "object literal: expected array or object value at path 'foo', found: null",
		},
		"check without single": {
			input: methods(
				jsonFn(`{"a":"first","b":"second"}`),
				method("without", "a"),
			),
			output: map[string]any{"b": "second"},
		},
		"check without double": {
			input: methods(
				jsonFn(`{"a":"first","b":"second","c":"third"}`),
				method("without", "a", "c"),
			),
			output: map[string]any{"b": "second"},
		},
		"check without nested": {
			input: methods(
				jsonFn(`{"inner":{"a":"first","b":"second","c":"third"}}`),
				method("without", "inner.a", "inner.c", "thisdoesntexist"),
			),
			output: map[string]any{
				"inner": map[string]any{"b": "second"},
			},
		},
		"check without combination": {
			input: methods(
				jsonFn(`{"d":"fourth","e":"fifth","inner":{"a":"first","b":"second","c":"third"}}`),
				method("without", "d", "inner.a", "inner.c"),
			),
			output: map[string]any{
				"e":     "fifth",
				"inner": map[string]any{"b": "second"},
			},
		},
		"check without nested not object": {
			input: methods(
				jsonFn(`{"a":"first","b":"second","c":"third"}`),
				method("without", "a", "c.foo"),
			),
			output: map[string]any{
				"b": "second",
				"c": "third",
			},
		},
		"check unique custom": {
			input: methods(
				jsonFn(`[{"v":"a"},{"v":"b"},{"v":"c"},{"v":"b"},{"v":"d"},{"v":"a"}]`),
				method("unique", NewFieldFunction("v")),
			),
			output: []any{
				map[string]any{"v": "a"},
				map[string]any{"v": "b"},
				map[string]any{"v": "c"},
				map[string]any{"v": "d"},
			},
		},
		"check unique bad": {
			input: methods(
				jsonFn(`[{"v":"a"},{"v":"b"},{"v":"c"},{"v":"b"},{"v":"d"},{"v":"a"}]`),
				method("unique"),
			),
			err: "array literal: index 0: expected string or number value, got object",
		},
		"check unique not array": {
			input: methods(
				literalFn("foo"),
				method("unique"),
			),
			err: "expected array value, got string from string literal (\"foo\")",
		},
		"check unique": {
			input: methods(
				jsonFn(`[3.0,5,3,4,5.1,5]`),
				method("unique"),
			),
			output: []any{3.0, 5.0, 4.0, 5.1},
		},
		"check unique strings": {
			input: methods(
				jsonFn(`["a","b","c","b","d","a"]`),
				method("unique"),
			),
			output: []any{"a", "b", "c", "d"},
		},
		"check unique mixed": {
			input: methods(
				jsonFn(`[3.0,"a","5",3,"b",5,"c","b",5.0,"d","a"]`),
				method("unique"),
			),
			output: []any{3.0, "a", "5", "b", 5.0, "c", "d"},
		},
		"check html escape query": {
			input: methods(
				literalFn("foo & bar"),
				method("escape_html"),
			),
			output: "foo &amp; bar",
		},
		"check html escape query bytes": {
			input: methods(
				function("content"),
				method("escape_html"),
			),
			messages: []easyMsg{
				{content: `foo & bar`},
			},
			output: "foo &amp; bar",
		},
		"check html unescape query": {
			input: methods(
				literalFn("foo &amp; bar"),
				method("unescape_html"),
			),
			output: "foo & bar",
		},
		"check html unescape query bytes": {
			input: methods(
				function(`content`),
				method("unescape_html"),
			),
			messages: []easyMsg{
				{content: `foo &amp; bar`},
			},
			output: "foo & bar",
		},
		"check sort custom": {
			input: methods(
				jsonFn(`[3,22,13,7,30]`),
				method("sort", arithmetic(NewFieldFunction("left"), NewFieldFunction("right"), ArithmeticGt)),
			),
			output: []any{30.0, 22.0, 13.0, 7.0, 3.0},
		},
		"check sort error": {
			input: methods(
				jsonFn(`[3,22,{"foo":"bar"},7,null]`),
				method("sort"),
			),
			err: "sort element 2: expected number or string value, got object",
		},
		"check sort strings custom": {
			input: methods(
				jsonFn(`["c","a","f","z"]`),
				method("sort", arithmetic(NewFieldFunction("left"), NewFieldFunction("right"), ArithmeticGt)),
			),
			output: []any{"z", "f", "c", "a"},
		},
		"check join": {
			input: methods(
				jsonFn(`["foo","bar"]`),
				method("join", ","),
			),
			output: "foo,bar",
		},
		"check join 2": {
			input: methods(
				jsonFn(`["foo"]`),
				method("join", ","),
			),
			output: "foo",
		},
		"check join 3": {
			input: methods(
				jsonFn(`[]`),
				method("join", ","),
			),
			output: "",
		},
		"check join no delim": {
			input: methods(
				jsonFn(`["foo","bar"]`),
				method("join"),
			),
			output: "foobar",
		},
		"check join fail not array": {
			input: methods(
				literalFn("foo"),
				method("join", ","),
			),
			err: "expected array value, got string from string literal (\"foo\")",
		},
		"check join fail number": {
			input: methods(
				jsonFn(`["foo",10,"bar"]`),
				method("join", ","),
			),
			err: "array literal: failed to join element 1: expected string value, got number (10)",
		},
		"check regexp find all submatch": {
			input: methods(
				literalFn("-axxb-ab-"),
				method("re_find_all_submatch", "a(x*)b"),
			),
			output: []any{
				[]any{"axxb", "xx"},
				[]any{"ab", ""},
			},
		},
		"check regexp find all submatch bytes": {
			input: methods(
				function(`content`),
				method("re_find_all_submatch", "a(x*)b"),
			),
			messages: []easyMsg{{content: `-axxb-ab-`}},
			output: []any{
				[]any{"axxb", "xx"},
				[]any{"ab", ""},
			},
		},
		"check regexp find all": {
			input: methods(
				literalFn("paranormal"),
				method("re_find_all", "a."),
			),
			output: []any{"ar", "an", "al"},
		},
		"check regexp find all bytes": {
			input: methods(
				function(`content`),
				method("re_find_all", "a."),
			),
			messages: []easyMsg{{content: `paranormal`}},
			output:   []any{"ar", "an", "al"},
		},
		"check type": {
			input: methods(
				literalFn("foobar"),
				method("type"),
			),
			output: "string",
		},
		"check has_prefix": {
			input: methods(
				literalFn("foobar"),
				method("has_prefix", "foo"),
			),
			output: true,
		},
		"check has_prefix 2": {
			input: methods(
				function("content"),
				method("has_prefix", "foo"),
			),
			messages: []easyMsg{{content: `foobar`}},
			output:   true,
		},
		"check has_prefix neg": {
			input: methods(
				literalFn("foobar"),
				method("has_prefix", "bar"),
			),
			output: false,
		},
		"check has_suffix": {
			input: methods(
				literalFn("foobar"),
				method("has_suffix", "bar"),
			),
			output: true,
		},
		"check has_suffix 2": {
			input: methods(
				function("content"),
				method("has_suffix", "bar"),
			),
			messages: []easyMsg{{content: `foobar`}},
			output:   true,
		},
		"check has_suffix neg": {
			input: methods(
				literalFn("foobar"),
				method("has_suffix", "foo"),
			),
			output: false,
		},
		"check bool": {
			input: methods(
				literalFn("true"),
				method("bool"),
			),
			output: true,
		},
		"check bool 2": {
			input: methods(
				literalFn("false"),
				method("bool"),
			),
			output: false,
		},
		"check bool 3": {
			input: methods(
				literalFn(true),
				method("bool"),
			),
			output: true,
		},
		"check bool 4": {
			input: methods(
				literalFn(false),
				method("bool"),
			),
			output: false,
		},
		"check bool 5": {
			input: methods(
				literalFn(int64(5)),
				method("bool"),
			),
			output: true,
		},
		"check bool 6": {
			input: methods(
				literalFn(int64(0)),
				method("bool"),
			),
			output: false,
		},
		"check bool 7": {
			input: methods(
				literalFn("nope"),
				method("bool"),
			),
			err: `expected bool value, got string from string literal ("nope")`,
		},
		"check bool 8": {
			input: methods(
				literalFn("nope"),
				method("bool", true),
			),
			output: true,
		},
		"check bool 9": {
			input: methods(
				literalFn("nope"),
				method("bool", false),
			),
			output: false,
		},
		"check number": {
			input: methods(
				literalFn("21"),
				method("number"),
			),
			output: float64(21),
		},
		"check number 2": {
			input: methods(
				literalFn("nope"),
				method("number"),
			),
			err: `string literal: strconv.ParseFloat: parsing "nope": invalid syntax`,
		},
		"check number 3": {
			input: methods(
				literalFn("nope"),
				method("number", 5.0),
			),
			output: float64(5),
		},
		"check number 4": {
			input: methods(
				literalFn("nope"),
				method("number", 5.2),
			),
			output: float64(5.2),
		},
		"check not_null": {
			input: methods(
				literalFn(21.0),
				method("not_null"),
			),
			output: 21.0,
		},
		"check not null 2": {
			input: methods(
				literalFn(nil),
				method("not_null"),
			),
			err: `null literal: value is null`,
		},
		"check index": {
			input: methods(
				jsonFn(`["foo","bar","baz"]`),
				method("index", int64(1)),
			),
			output: "bar",
		},
		"check index neg": {
			input: methods(
				jsonFn(`["foo","bar","baz"]`),
				method("index", int64(-1)),
			),
			output: "baz",
		},
		"check index oob": {
			input: methods(
				jsonFn(`["foo","bar","baz"]`),
				method("index", int64(4)),
				method("catch", "buz"),
			),
			output: "buz",
		},
		"check index oob neg": {
			input: methods(
				jsonFn(`["foo","bar","baz"]`),
				method("index", int64(-4)),
				method("catch", "buz"),
			),
			output: "buz",
		},
		"check url escape query": {
			input: methods(
				literalFn("foo & bar"),
				method("escape_url_query"),
			),
			output: "foo+%26+bar",
		},
		"check url escape query bytes": {
			input: methods(
				function("content"),
				method("escape_url_query"),
			),
			messages: []easyMsg{
				{content: `foo & bar`},
			},
			output: "foo+%26+bar",
		},
		"check url unescape query": {
			input: methods(
				literalFn("foo+%26+bar"),
				method("unescape_url_query"),
			),
			output: "foo & bar",
		},
		"check url unescape query bytes": {
			input: methods(
				function("content"),
				method("unescape_url_query"),
			),
			messages: []easyMsg{
				{content: `foo+%26+bar`},
			},
			output: "foo & bar",
		},
		"check flatten": {
			input: methods(
				function("json"),
				method("flatten"),
			),
			messages: []easyMsg{
				{content: `["foo",["bar","baz"],"buz"]`},
			},
			output: []any{
				"foo", "bar", "baz", "buz",
			},
		},
		"check flatten 2": {
			input: methods(
				function("json"),
				method("flatten"),
			),
			messages: []easyMsg{
				{content: `[]`},
			},
			output: []any{},
		},
		"check flatten 3": {
			input: methods(
				function("json"),
				method("flatten"),
			),
			messages: []easyMsg{
				{content: `["foo","bar","baz","buz"]`},
			},
			output: []any{
				"foo", "bar", "baz", "buz",
			},
		},
		"check collapse": {
			input: methods(
				function("json"),
				method("collapse"),
			),
			messages: []easyMsg{
				{content: `{"foo":[{"bar":"1"},{"bar":{}},{"bar":"2"},{"bar":[]}]}`},
			},
			output: map[string]any{
				"foo.0.bar": "1",
				"foo.2.bar": "2",
			},
		},
		"check collapse include empty": {
			input: methods(
				function("json"),
				method("collapse", true),
			),
			messages: []easyMsg{
				{content: `{"foo":[{"bar":"1"},{"bar":{}},{"bar":"2"},{"bar":[]}]}`},
			},
			output: map[string]any{
				"foo.0.bar": "1",
				"foo.1.bar": struct{}{},
				"foo.2.bar": "2",
				"foo.3.bar": []struct{}{},
			},
		},
		"check sha1 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "sha1"),
				method("encode", "hex"),
			),
			output: `2aae6c35c94fcfb415dbe95f408b9ce91ee846ed`,
		},
		"check hmac sha1 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "hmac_sha1", "static-key"),
				method("encode", "hex"),
			),
			output: `d87e5f068fa08fe90bb95bc7c8344cb809179d76`,
		},
		"check hmac sha1 hash 2": {
			input: methods(
				literalFn("hello world"),
				method("hash", "hmac_sha1", "foo"),
				method("encode", "hex"),
			),
			output: `20224529cc42a39bacc96459f6ead9d17da7f128`,
		},
		"check sha256 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "sha256"),
				method("encode", "hex"),
			),
			output: `b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9`,
		},
		"check hmac sha256 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "hmac_sha256", "static-key"),
				method("encode", "hex"),
			),
			output: `b1cdce8b2add1f96135b2506f8ab748ae8ef15c49c0320357a6d168c42e20746`,
		},
		"check sha512 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "sha512"),
				method("encode", "hex"),
			),
			output: `309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f`,
		},
		"check hmac sha512 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "hmac_sha512", "static-key"),
				method("encode", "hex"),
			),
			output: `fd5d5ed60b96e820ebaace4fed962a401adefd3e89c51a374f0bb7f49ed02892af8bc8591628dcbc8b5f065df6bb06588cba95d488c1c8b88faa7cbe08e4558d`,
		},
		"check xxhash64 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "xxhash64"),
				method("string"),
			),
			output: `5020219685658847592`,
		},
		"check md5 hash": {
			input: methods(
				literalFn("hello world"),
				method("hash", "md5"),
				method("encode", "hex"),
			),
			output: `5eb63bbbe01eeed093cb22bb8f5acdc3`,
		},
		"check crc32 hash IEEE (default)": {
			input: methods(
				literalFn("hello world"),
				method("hash", "crc32"),
				method("encode", "hex"),
			),
			output: `0d4a1185`,
		},
		"check crc32 hash IEEE (explicit)": {
			input: methods(
				literalFn("hello world"),
				method("hash", "crc32", "", "IEEE"),
				method("encode", "hex"),
			),
			output: `0d4a1185`,
		},
		"check crc32 hash Castagnoli": {
			input: methods(
				literalFn("hello world"),
				method("hash", "crc32", "", "Castagnoli"),
				method("encode", "hex"),
			),
			output: `c99465aa`,
		},
		"check crc32 hash Koopman": {
			input: methods(
				literalFn("hello world"),
				method("hash", "crc32", "", "Koopman"),
				method("encode", "hex"),
			),
			output: `df373d3c`,
		},
		"check crc32 hash not supported": {
			input: methods(
				literalFn("hello world"),
				method("hash", "crc32", "", "not-supported"),
				method("encode", "hex"),
			),
			err: `string literal: unsupported crc32 hash key "not-supported"`,
		},
		"check hex encode": {
			input: methods(
				literalFn("hello world"),
				method("encode", "hex"),
			),
			output: `68656c6c6f20776f726c64`,
		},
		"check hex decode": {
			input: methods(
				literalFn("68656c6c6f20776f726c64"),
				method("decode", "hex"),
				method("string"),
			),
			output: `hello world`,
		},
		"check base64 encode": {
			input: methods(
				literalFn("hello world"),
				method("encode", "base64"),
			),
			output: `aGVsbG8gd29ybGQ=`,
		},
		"check base64 decode": {
			input: methods(
				literalFn("aGVsbG8gd29ybGQ="),
				method("decode", "base64"),
				method("string"),
			),
			output: `hello world`,
		},
		"check base64url encode": {
			input: methods(
				literalFn("<<???>>"),
				method("encode", "base64url"),
			),
			output: `PDw_Pz8-Pg==`,
		},
		"check base64url decode": {
			input: methods(
				literalFn("PDw_Pz8-Pg=="),
				method("decode", "base64url"),
				method("string"),
			),
			output: `<<???>>`,
		},
		"check z85 encode": {
			input: methods(
				literalFn("hello world!"),
				method("encode", "z85"),
			),
			output: `xK#0@zY<mxA+]nf`,
		},
		"check z85 decode": {
			input: methods(
				literalFn("xK#0@zY<mxA+]nf"),
				method("decode", "z85"),
				method("string"),
			),
			output: `hello world!`,
		},
		/*
			"check z85 encode misaligned": {
				input: methods(
					literalFn("hello world"),
					method("encode", "z85"),
				),
				output: `xK#0@zY<mxA+]m`,
			},
			"check z85 decode misaligned": {
				input: methods(
					literalFn("xK#0@zY<mxA+]m"),
					method("decode", "z85"),
					method("string"),
				),
				output: `hello world`,
			},
		*/
		"check ascii85 encode": {
			input: methods(
				literalFn("hello world!"),
				method("encode", "ascii85"),
			),
			output: `BOu!rD]j7BEbo80`,
		},
		"check ascii85 decode": {
			input: methods(
				literalFn("BOu!rD]j7BEbo80"),
				method("decode", "ascii85"),
				method("string"),
			),
			output: `hello world!`,
		},
		"check ascii85 encode misaligned": {
			input: methods(
				literalFn("hello world"),
				method("encode", "ascii85"),
			),
			output: `BOu!rD]j7BEbo7`,
		},
		"check ascii85 decode misaligned": {
			input: methods(
				literalFn("BOu!rD]j7BEbo7"),
				method("decode", "ascii85"),
				method("string"),
			),
			output: `hello world`,
		},
		"check hex encode bytes": {
			input: methods(
				function("content"),
				method("encode", "hex"),
			),
			messages: []easyMsg{
				{content: `hello world`},
			},
			output: `68656c6c6f20776f726c64`,
		},
		"check strip html": {
			input: methods(
				literalFn("<p>the plain <strong>old text</strong></p>"),
				method("strip_html"),
			),
			output: `the plain old text`,
		},
		"check strip html bytes": {
			input: methods(
				function("content"),
				method("strip_html"),
			),
			messages: []easyMsg{
				{content: `<p>the plain <strong>old text</strong></p>`},
			},
			output: []byte(`the plain old text`),
		},
		"check quote": {
			input: methods(
				NewFieldFunction(""),
				method("quote"),
			),
			value: func() *any {
				var s any = linebreakStr
				return &s
			}(),
			output: `"foo\nbar\nbaz"`,
		},
		"check quote bytes": {
			input: methods(
				NewFieldFunction(""),
				method("quote"),
			),
			value: func() *any {
				var s any = []byte(linebreakStr)
				return &s
			}(),
			output: `"foo\nbar\nbaz"`,
		},
		"check unquote": {
			input: methods(
				NewFieldFunction(""),
				method("unquote"),
			),
			value: func() *any {
				var s any = "\"foo\\nbar\\nbaz\""
				return &s
			}(),
			output: linebreakStr,
		},
		"check unquote bytes": {
			input: methods(
				NewFieldFunction(""),
				method("unquote"),
			),
			value: func() *any {
				var s any = []byte("\"foo\\nbar\\nbaz\"")
				return &s
			}(),
			output: linebreakStr,
		},
		"check replace": {
			input: methods(
				literalFn("The foo ate my homework"),
				method("replace_all", "foo", "dog"),
			),
			output: "The dog ate my homework",
		},
		"check replace bytes": {
			input: methods(
				function("content"),
				method("replace_all", "foo", "dog"),
			),
			messages: []easyMsg{
				{content: `The foo ate my homework`},
			},
			output: []byte("The dog ate my homework"),
		},
		"check trim": {
			input: methods(
				literalFn(" the foo bar   "),
				method("trim"),
			),
			output: "the foo bar",
		},
		"check trim 2": {
			input: methods(
				literalFn("!!?!the foo bar!"),
				method("trim", "!?"),
			),
			output: "the foo bar",
		},
		"check trim bytes": {
			input: methods(
				function(`content`),
				method("trim"),
			),
			messages: []easyMsg{
				{content: `  the foo bar  `},
			},
			output: []byte("the foo bar"),
		},
		"check trim bytes 2": {
			input: methods(
				function(`content`),
				method("trim", "!?"),
			),
			messages: []easyMsg{
				{content: `!!?!the foo bar!`},
			},
			output: []byte("the foo bar"),
		},
		"check capitalize": {
			input: methods(
				literalFn("the foo bar"),
				method("capitalize"),
			),
			output: "The Foo Bar",
		},
		"check capitalize bytes": {
			input: methods(
				function(`content`),
				method("capitalize"),
			),
			messages: []easyMsg{
				{content: `the foo bar`},
			},
			output: []byte("The Foo Bar"),
		},
		"check split": {
			input: methods(
				literalFn("foo,bar,baz"),
				method("split", ","),
			),
			output: []any{"foo", "bar", "baz"},
		},
		"check split bytes": {
			input: methods(
				function("content"),
				method("split", ","),
			),
			messages: []easyMsg{
				{content: `foo,bar,baz,`},
			},
			output: []any{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("")},
		},
		"check slice": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", 0.0, 3.0),
			),
			output: "foo",
		},
		"check slice 2": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", 8.0),
			),
			output: "baz",
		},
		"check slice neg start": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", -1.0),
			),
			output: "z",
		},
		"check slice neg start 2": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", -2.0),
			),
			output: "az",
		},
		"check slice neg start 3": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", -100.0),
			),
			output: "foo bar baz",
		},
		"check slice neg end 1": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", 0.0, -1.0),
			),
			output: "foo bar ba",
		},
		"check slice neg end 2": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", 0.0, -2.0),
			),
			output: "foo bar b",
		},
		"check slice neg end 3": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", 0.0, -100.0),
			),
			output: "",
		},
		"check slice oob string": {
			input: methods(
				literalFn("foo bar baz"),
				method("slice", 0.0, 30.0),
			),
			output: "foo bar baz",
		},
		"check slice oob array": {
			input: methods(
				jsonFn(`["foo","bar","baz"]`),
				method("slice", 0.0, 30.0),
			),
			output: []any{"foo", "bar", "baz"},
		},
		"check slice invalid": {
			input: methods(
				literalFn(10.0),
				method("slice", 8.0),
			),
			err: `expected array or string value, got number from number literal (10)`,
		},
		"check slice array": {
			input: methods(
				jsonFn(`["foo","bar","baz","buz"]`),
				method("slice", 1.0, 3.0),
			),
			output: []any{"bar", "baz"},
		},
		"check regexp match": {
			input: methods(
				literalFn(`"there are 10 puppies"`),
				method("re_match", "[0-9]"),
			),
			output: true,
		},
		"check regexp match 2": {
			input: methods(
				literalFn(`"there are ten puppies"`),
				method("re_match", "[0-9]"),
			),
			output: false,
		},
		"check regexp match dynamic": {
			input: methods(
				function("json", "input"),
				method("re_match", function("json", "re")),
			),
			messages: []easyMsg{
				{content: `{"input":"there are 10 puppies","re":"[0-9]"}`},
			},
			output: true,
		},
		"check regexp replace": {
			input: methods(
				literalFn("foo ADD 70"),
				method("re_replace_all", "ADD ([0-9]+)", "+($1)"),
			),
			output: "foo +(70)",
		},
		"check regexp replace dynamic": {
			input: methods(
				function("json", "input"),
				method("re_replace_all", function("json", "re"), function("json", "replace")),
			),
			messages: []easyMsg{
				{content: `{"input":"foo ADD 70","re":"ADD ([0-9]+)","replace":"+($1)"}`},
			},
			output: "foo +(70)",
		},
		"check parse json": {
			input: methods(
				literalFn("{\"foo\":\"bar\"}"),
				method("parse_json"),
			),
			output: map[string]any{
				"foo": "bar",
			},
		},
		"check parse json with use_number set to true": {
			input: methods(
				literalFn("{\"foo\":11380878173205700000000000000000000000000000000}"),
				method("parse_json", true),
			),
			output: map[string]any{
				"foo": json.Number("11380878173205700000000000000000000000000000000"),
			},
		},
		"check parse json with use_number set to false": {
			input: methods(
				literalFn("{\"foo\":11380878173205700000000000000000000000000000000}"),
				method("parse_json", false),
			),
			output: map[string]any{
				"foo": 1.13808781732057e+46,
			},
		},
		"check parse json with use_number not set": {
			input: methods(
				literalFn("{\"foo\":11380878173205700000000000000000000000000000000}"),
				method("parse_json"),
			),
			output: map[string]any{
				"foo": 1.13808781732057e+46,
			},
		},
		"check parse json invalid": {
			input: methods(
				literalFn("not valid json"),
				method("parse_json"),
			),
			err: `string literal: failed to parse value as JSON: invalid character 'o' in literal null (expecting 'u')`,
		},
		"check append": {
			input: methods(
				jsonFn(`["foo"]`),
				method("append", "bar", "baz"),
			),
			output: []any{
				"foo", "bar", "baz",
			},
		},
		"check append 2": {
			input: methods(
				jsonFn(`["foo"]`),
				method("map", methods(
					NewFieldFunction(""),
					method("append", NewFieldFunction("")),
				)),
			),
			output: []any{
				"foo", []any{"foo"},
			},
		},
		"check enumerated": {
			input: methods(
				jsonFn(`["foo","bar","baz"]`),
				method("enumerated"),
			),
			output: []any{
				map[string]any{
					"index": int64(0),
					"value": "foo",
				},
				map[string]any{
					"index": int64(1),
					"value": "bar",
				},
				map[string]any{
					"index": int64(2),
					"value": "baz",
				},
			},
		},
		"check merge": {
			input: methods(
				jsonFn(`{"foo":"val1"}`),
				method("merge", jsonFn(`{"bar":"val2"}`)),
			),
			output: map[string]any{
				"foo": "val1",
				"bar": "val2",
			},
		},
		"check merge 2": {
			input: methods(
				function("json"),
				method("map", methods(
					NewFieldFunction("foo"),
					method("merge", NewFieldFunction("bar")),
				)),
			),
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":6},"foo":{"first":"val1","third":3}}`},
			},
			output: map[string]any{
				"first":  "val1",
				"second": "val2",
				"third":  []any{jn(3), jn(6)},
			},
		},
		"check merge 3": {
			input: methods(
				function("json"),
				method("map", methods(
					NewFieldFunction(""),
					method("merge", NewFieldFunction("bar")),
				)),
			),
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":6},"foo":{"first":"val1","third":3}}`},
			},
			output: map[string]any{
				"foo": map[string]any{
					"first": "val1",
					"third": jn(3),
				},
				"bar": map[string]any{
					"second": "val2",
					"third":  jn(6),
				},
				"second": "val2",
				"third":  jn(6),
			},
		},
		"check merge 4": {
			input: methods(
				function("json"),
				method("map", methods(
					NewFieldFunction("foo"),
					method("merge", NewFieldFunction("bar")),
				)),
			),
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":[6]},"foo":{"first":"val1","third":[3]}}`},
			},
			output: map[string]any{
				"first":  "val1",
				"second": "val2",
				"third":  []any{jn(3), jn(6)},
			},
		},
		"check merge 5": {
			input: methods(
				function("json"),
				method("map", methods(
					NewFieldFunction("foo"),
					method("merge", NewFieldFunction("bar")),
					method("merge", NewFieldFunction("foo")),
				)),
			),
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":[6]},"foo":{"first":"val1","third":[3]}}`},
			},
			output: map[string]any{
				"first":  []any{"val1", "val1"},
				"second": "val2",
				"third":  []any{jn(3), jn(6), jn(3)},
			},
		},
		"check merge arrays": {
			input: methods(
				jsonFn("[]"),
				method("merge", "foo"),
			),
			messages: []easyMsg{
				{content: `{}`},
			},
			output: []any{"foo"},
		},
		"check merge arrays 2": {
			input: methods(
				jsonFn(`["foo"]`),
				method("merge", []any{"bar", "baz"}),
			),
			messages: []easyMsg{
				{content: `{}`},
			},
			output: []any{"foo", "bar", "baz"},
		},
		"check contains array": {
			input: methods(
				function("json"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `["nope","foo","bar"]`}},
			output:   true,
		},
		"check contains array 2": {
			input: methods(
				function("json"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `["nope","bar"]`}},
			output:   false,
		},
		"check contains array nums": {
			input: methods(
				function("json"),
				method("contains", int64(10)),
			),
			messages: []easyMsg{{content: `["nope",10.0,3]`}},
			output:   true,
		},
		"check contains array nums 2": {
			input: methods(
				function("json"),
				method("contains", int64(10)),
			),
			messages: []easyMsg{{content: `["nope",3]`}},
			output:   false,
		},
		"check contains map": {
			input: methods(
				function("json"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `{"1":"nope","2":"foo","3":"bar"}`}},
			output:   true,
		},
		"check contains map 2": {
			input: methods(
				function("json"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `{"1":"nope","3":"bar"}`}},
			output:   false,
		},
		"check contains invalid type": {
			input: methods(
				function("json", "nope"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `{"nope":false}`}},
			err:      "expected string, array or object value, got bool from json path `nope` (false)",
		},
		"check substr": {
			input: methods(
				function("json", "foo"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `{"foo":"hello foo world"}`}},
			output:   true,
		},
		"check substr 2": {
			input: methods(
				function("json", "foo"),
				method("contains", "foo"),
			),
			messages: []easyMsg{{content: `{"foo":"hello bar world"}`}},
			output:   false,
		},
		"check map each": {
			input: methods(
				jsonFn(`["foo","bar"]`),
				method("map_each", methods(
					NewFieldFunction(""),
					method("uppercase"),
				)),
			),
			output: []any{"FOO", "BAR"},
		},
		"check map each 2": {
			input: methods(
				jsonFn(`["foo","bar"]`),
				method("map_each", methods(
					literalFn("(%v)"),
					method("format", NewFieldFunction("")),
					method("uppercase"),
				)),
			),
			output: []any{"(FOO)", "(BAR)"},
		},
		"check map each object": {
			input: methods(
				jsonFn(`{"foo":"hello world","bar":"this is ash"}`),
				method("map_each", methods(
					NewFieldFunction("value"),
					method("uppercase"),
				)),
			),
			output: map[string]any{
				"foo": "HELLO WORLD",
				"bar": "THIS IS ASH",
			},
		},
		"check filter array": {
			input: methods(
				jsonFn(`[2,14,4,11,7]`),
				method("filter", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", 10.0),
					ArithmeticGt,
				)),
			),
			output: []any{14.0, 11.0},
		},
		"check filter object": {
			input: methods(
				jsonFn(`{"foo":"hello ! world","bar":"this is ash","baz":"im cool!"}`),
				method("filter", methods(
					NewFieldFunction("value"),
					method("contains", "!"),
				)),
			),
			output: map[string]any{
				"foo": "hello ! world",
				"baz": "im cool!",
			},
		},
		"check fold": {
			input: methods(
				jsonFn(`[3,5,2]`),
				method("fold", 0.0, arithmetic(
					NewFieldFunction("tally"),
					NewFieldFunction("value"),
					ArithmeticAdd,
				)),
			),
			messages: []easyMsg{
				{content: `{}`},
			},
			output: float64(10),
		},
		"check fold 2": {
			input: methods(
				jsonFn(`["foo","bar"]`),
				method("fold", "", methods(
					literalFn("%v%v"),
					method("format", NewFieldFunction("tally"), NewFieldFunction("value")),
				)),
			),
			messages: []easyMsg{
				{content: `{}`},
			},
			output: "foobar",
		},
		"check fold exec err 2": {
			input: methods(
				jsonFn(`["foo","bar"]`),
				method("fold", jsonFn(`{"values":[]}`), methods(
					NewFieldFunction("does.not.exist"),
					method("number"),
				)),
			),
			messages: []easyMsg{
				{content: `{}`},
			},
			err: "expected number value, got null from field `this.does.not.exist`",
		},
		"check keys literal": {
			input: methods(
				jsonFn(`{"foo":1,"bar":2}`),
				method("keys"),
				method("sort"),
			),
			messages: []easyMsg{{content: `{}`}},
			output:   []any{"bar", "foo"},
		},
		"check keys empty": {
			input: methods(
				jsonFn(`{}`),
				method("keys"),
			),
			messages: []easyMsg{{content: `{}`}},
			output:   []any{},
		},
		"check keys function": {
			input: methods(
				function(`json`),
				method("keys"),
				method("sort"),
			),
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			output:   []any{"bar", "foo"},
		},
		"check keys error": {
			input: methods(
				literalFn(`foo`),
				method("keys"),
				method("sort"),
			),
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			err:      `expected object value, got string from string literal ("foo")`,
		},
		"check values literal": {
			input: methods(
				jsonFn(`{"foo":1,"bar":2}`),
				method("values"),
				method("sort"),
			),
			messages: []easyMsg{{content: `{}`}},
			output:   []any{1.0, 2.0},
		},
		"check values empty": {
			input: methods(
				jsonFn(`{}`),
				method("values"),
			),
			messages: []easyMsg{{content: `{}`}},
			output:   []any{},
		},
		"check values function": {
			input: methods(
				function(`json`),
				method("values"),
				method("sort"),
			),
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			output:   []any{jn(1), jn(2)},
		},
		"check values error": {
			input: methods(
				literalFn(`foo`),
				method("values"),
				method("sort"),
			),
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			err:      `expected object value, got string from string literal ("foo")`,
		},
		"check aes-ctr encryption": {
			input: methods(
				literalFn("hello world!"),
				method(
					"encrypt_aes", "ctr",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"),
						method("decode", "hex"),
					),
				),
				method("encode", "hex"),
			),
			output: `84e9b31ff7400bdf80be7254`,
		},
		"check aes-ctr decryption": {
			input: methods(
				literalFn("84e9b31ff7400bdf80be7254"),
				method("decode", "hex"),
				method(
					"decrypt_aes", "ctr",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"),
						method("decode", "hex"),
					),
				),
				method("string"),
			),
			output: `hello world!`,
		},
		"check aes-ofb encryption": {
			input: methods(
				literalFn("hello world!"),
				method(
					"encrypt_aes", "ofb",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("000102030405060708090a0b0c0d0e0f"),
						method("decode", "hex"),
					),
				),
				method("encode", "hex"),
			),
			output: `389b0ba0f64d45d9a86553c8`,
		},
		"check aes-ofb decryption": {
			input: methods(
				literalFn("389b0ba0f64d45d9a86553c8"),
				method("decode", "hex"),
				method(
					"decrypt_aes", "ofb",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("000102030405060708090a0b0c0d0e0f"),
						method("decode", "hex"),
					),
				),
				method("string"),
			),
			output: `hello world!`,
		},
		"check aes-cbc encryption": {
			input: methods(
				literalFn("6bc1bee22e409f96e93d7e117393172a"),
				method("decode", "hex"),
				method(
					"encrypt_aes", "cbc",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("000102030405060708090a0b0c0d0e0f"),
						method("decode", "hex"),
					),
				),
				method("encode", "hex"),
			),
			output: `7649abac8119b246cee98e9b12e9197d`,
		},
		"check aes-cbc encryption error": {
			input: methods(
				literalFn("hello world"),
				method(
					"encrypt_aes", "cbc",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("000102030405060708090a0b0c0d0e0f"),
						method("decode", "hex"),
					),
				),
				method("encode", "hex"),
			),
			err: `string literal: plaintext is not a multiple of the block size`,
		},
		"check aes-cbc decryption": {
			input: methods(
				literalFn("7649abac8119b246cee98e9b12e9197d"),
				method("decode", "hex"),
				method(
					"decrypt_aes", "cbc",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("000102030405060708090a0b0c0d0e0f"),
						method("decode", "hex"),
					),
				),
				method("string"),
				method("encode", "hex"),
			),
			output: `6bc1bee22e409f96e93d7e117393172a`,
		},
		"check aes-cbc decryption error": {
			input: methods(
				literalFn("7649abac81"),
				method("decode", "hex"),
				method(
					"decrypt_aes", "cbc",
					methods(
						literalFn("2b7e151628aed2a6abf7158809cf4f3c"),
						method("decode", "hex"),
					),
					methods(
						literalFn("000102030405060708090a0b0c0d0e0f"),
						method("decode", "hex"),
					),
				),
				method("string"),
				method("encode", "hex"),
			),
			err: `method decode: ciphertext is not a multiple of the block size`,
		},
		"check any no array": {
			input: methods(
				literalFn("foo"),
				method("any", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", "bar"),
					ArithmeticEq,
				)),
			),
			err: "expected array value, got string from string literal (\"foo\")",
		},
		"check any bad mapping": {
			input: methods(
				literalFn([]any{false, "bar", true}),
				method("any", NewFieldFunction("")),
			),
			err: "array literal: element 1: expected bool value, got string (\"bar\")",
		},
		"check any true": {
			input: methods(
				literalFn([]any{"foo", "bar", "baz"}),
				method("any", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", "bar"),
					ArithmeticEq,
				)),
			),
			output: true,
		},
		"check any false": {
			input: methods(
				literalFn([]any{"foo", "buz", "baz"}),
				method("any", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", "bar"),
					ArithmeticEq,
				)),
			),
			output: false,
		},
		"check any empty": {
			input: methods(
				literalFn([]any{}),
				method("any", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", 9.0),
					ArithmeticLt,
				)),
			),
			output: false,
		},
		"check all true": {
			input: methods(
				literalFn([]any{10.0, 11.0, 12.0}),
				method("all", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", 9.0),
					ArithmeticGt,
				)),
			),
			output: true,
		},
		"check all false": {
			input: methods(
				literalFn([]any{10.0, 8.0, 12.0}),
				method("all", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", 9.0),
					ArithmeticGt,
				)),
			),
			output: false,
		},
		"check all empty": {
			input: methods(
				literalFn([]any{}),
				method("all", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", 9.0),
					ArithmeticLt,
				)),
			),
			output: false,
		},
		"check all bad mapping": {
			input: methods(
				literalFn([]any{true, "bar", false}),
				method("all", NewFieldFunction("")),
			),
			err: "array literal: element 1: expected bool value, got string (\"bar\")",
		},
		"check all no array": {
			input: methods(
				literalFn("foo"),
				method("any", arithmetic(
					NewFieldFunction(""),
					NewLiteralFunction("", "bar"),
					ArithmeticEq,
				)),
			),
			err: "expected array value, got string from string literal (\"foo\")",
		},
		"check floor": {
			input:  methods(literalFn(5.8), method("floor")),
			output: int64(5),
		},
		"check floor bad value": {
			input: methods(literalFn("nope"), method("floor")),
			err:   "expected number value, got string from string literal (\"nope\")",
		},
		"check floor int": {
			input:  methods(literalFn(int64(5)), method("floor")),
			output: int64(5),
		},
		"check floor uint": {
			input:  methods(literalFn(uint64(5)), method("floor")),
			output: uint64(5),
		},
		"check floor json.Number": {
			input:  methods(literalFn(json.Number("5.8")), method("floor")),
			output: int64(5),
		},
		"check round up": {
			input:  methods(literalFn(5.8), method("round")),
			output: int64(6),
		},
		"check round down": {
			input:  methods(literalFn(5.3), method("round")),
			output: int64(5),
		},
		"check replace_many string": {
			input: methods(literalFn("<i>hello</i> <b>world</b>"), method("replace_all_many", []any{
				"<b>", "BOLD",
				"</b>", "!BOLD",
				"<i>", "ITA",
				"</i>", "!ITA",
			})),
			output: "ITAhello!ITA BOLDworld!BOLD",
		},
		"check replace_many bytes": {
			input: methods(literalFn([]byte("<i>hello</i> <b>world</b>")), method("replace_all_many", []any{
				"<b>", "BOLD",
				"</b>", "!BOLD",
				"<i>", "ITA",
				"</i>", "!ITA",
			})),
			output: []byte("ITAhello!ITA BOLDworld!BOLD"),
		},
		"check index of": {
			input: methods(
				function(`content`),
				method("index_of", "bar"),
			),
			messages: []easyMsg{
				{content: `foobar`},
			},
			output: int64(3),
		},
		"check index of no match": {
			input: methods(
				function(`content`),
				method("index_of", "bar"),
			),
			messages: []easyMsg{
				{content: `foofoo`},
			},
			output: int64(-1),
		},
		"check reverse": {
			input: methods(
				function(`content`),
				method("reverse"),
			),
			messages: []easyMsg{
				{content: `foobar`},
			},
			output: []byte("raboof"),
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
				msg = append(msg, part)
			}

			for i := 0; i < 10; i++ {
				res, err := test.input.Exec(FunctionContext{
					Maps:     map[string]Function{},
					Index:    test.index,
					MsgBatch: msg,
				}.WithValueFunc(func() *any { return test.value }))
				if len(test.err) > 0 {
					require.EqualError(t, err, test.err)
				} else {
					require.NoError(t, err)
				}
				require.Equal(t, test.output, res)
			}

			// Ensure nothing changed
			for i, m := range test.messages {
				doc, err := msg.Get(i).AsStructuredMut()
				if err == nil {
					msg.Get(i).SetStructured(doc)
				}
				assert.Equal(t, m.content, string(msg.Get(i).AsBytes()))
			}
		})
	}
}

func TestMethodTargets(t *testing.T) {
	function := func(name string, args ...any) Function {
		t.Helper()
		fn, err := InitFunctionHelper(name, args...)
		require.NoError(t, err)
		return fn
	}
	method := func(fn Function, name string, args ...any) Function {
		t.Helper()
		fn, err := InitMethodHelper(name, fn, args...)
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		input  Function
		maps   map[string]Function
		output []TargetPath
	}{
		"get from json": {
			input: method(function("json", "foo.bar"), "get", "baz.buz"),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetValue, "foo", "bar", "baz", "buz"),
			},
		},
		"get from get from json": {
			input: method(method(function("json", "foo.bar"), "get", "baz"), "get", "buz"),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetValue, "foo", "bar", "baz", "buz"),
			},
		},
		"mapping get from json": {
			input: method(NewFieldFunction("foo.bar"), "map", NewFieldFunction("baz")),
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetValue, "foo", "bar", "baz"),
			},
		},
		"ref mapping get from json": {
			input: method(NewFieldFunction("foo.bar"), "apply", "foomap"),
			maps: map[string]Function{
				"foomap": NewFieldFunction("baz"),
			},
			output: []TargetPath{
				NewTargetPath(TargetValue, "foo", "bar"),
				NewTargetPath(TargetValue, "foo", "bar", "baz"),
			},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, res := test.input.QueryTargets(TargetsContext{
				Maps: test.maps,
			})
			assert.Equal(t, test.output, res)
		})
	}
}

func TestMethodNoArgsTargets(t *testing.T) {
	fn := NewFieldFunction("foo.bar.baz")
	exp := NewTargetPath(TargetValue, "foo", "bar", "baz")
	for k := range AllMethods.constructors {
		// Only tests methods that do not need arguments, we need manual checks
		// for other methods.
		m, err := InitMethodHelper(k, fn)
		if err != nil {
			continue
		}
		_, targets := m.QueryTargets(TargetsContext{
			Maps: map[string]Function{},
		})
		assert.Contains(t, targets, exp, "method: %v", k)
	}
}
