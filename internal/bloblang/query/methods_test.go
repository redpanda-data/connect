package query

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var linebreakStr = `foo
bar
baz`

func TestMethods(t *testing.T) {
	type easyMsg struct {
		content string
		meta    map[string]string
	}

	tests := map[string]struct {
		input    string
		value    *interface{}
		output   interface{}
		err      string
		messages []easyMsg
		index    int
	}{
		"check parse csv 1": {
			input: `"foo,bar,baz\n1,2,3\n4,5,6".parse_csv()`,
			output: []interface{}{
				map[string]interface{}{
					"foo": "1",
					"bar": "2",
					"baz": "3",
				},
				map[string]interface{}{
					"foo": "4",
					"bar": "5",
					"baz": "6",
				},
			},
		},
		"check parse csv 2": {
			input:  `"foo,bar,baz".parse_csv()`,
			output: []interface{}{},
		},
		"check parse csv 3": {
			input:  `"foo,bar\nfoo 1,bar 1\nfoo 2,bar 2".parse_csv().string()`,
			output: `[{"bar":"bar 1","foo":"foo 1"},{"bar":"bar 2","foo":"foo 2"}]`,
		},
		"check parse csv error 1": {
			input: `"foo,bar,baz\n1,2,3,4".parse_csv()`,
			err:   "record on line 2: wrong number of fields",
		},
		"check explode 1": {
			input:  `{"foo":[1,2,3],"id":"bar"}.explode("foo").string()`,
			output: `[{"foo":1,"id":"bar"},{"foo":2,"id":"bar"},{"foo":3,"id":"bar"}]`,
		},
		"check explode 2": {
			input:  `{"foo":{"also":"this","bar":[{"key":"value1"},{"key":"value2"},{"key":"value3"}]},"id":"baz"}.explode("foo.bar").string()`,
			output: `[{"foo":{"also":"this","bar":{"key":"value1"}},"id":"baz"},{"foo":{"also":"this","bar":{"key":"value2"}},"id":"baz"},{"foo":{"also":"this","bar":{"key":"value3"}},"id":"baz"}]`,
		},
		"check explode 3": {
			input:  `{"foo":{"a":1,"b":2,"c":3},"id":"bar"}.explode("foo").string()`,
			output: `{"a":{"foo":1,"id":"bar"},"b":{"foo":2,"id":"bar"},"c":{"foo":3,"id":"bar"}}`,
		},
		"check explode 4": {
			input:  `{"foo":{"also":"this","bar":{"key1":["a","b"],"key2":{"c":3,"d":4}}},"id":"baz"}.explode("foo.bar").string()`,
			output: `{"key1":{"foo":{"also":"this","bar":["a","b"]},"id":"baz"},"key2":{"foo":{"also":"this","bar":{"c":3,"d":4}},"id":"baz"}}`,
		},
		"check without single": {
			input:  `{"a":"first","b":"second"}.without("a")`,
			output: map[string]interface{}{"b": "second"},
		},
		"check without double": {
			input:  `{"a":"first","b":"second","c":"third"}.without("a","c")`,
			output: map[string]interface{}{"b": "second"},
		},
		"check without nested": {
			input: `{"inner":{"a":"first","b":"second","c":"third"}}.without("inner.a","inner.c","thisdoesntexist")`,
			output: map[string]interface{}{
				"inner": map[string]interface{}{"b": "second"},
			},
		},
		"check without combination": {
			input: `{"d":"fourth","e":"fifth","inner":{"a":"first","b":"second","c":"third"}}.without("d","inner.a","inner.c")`,
			output: map[string]interface{}{
				"e":     "fifth",
				"inner": map[string]interface{}{"b": "second"},
			},
		},
		"check without nested not object": {
			input: `{"a":"first","b":"second","c":"third"}.without("a","c.foo")`,
			output: map[string]interface{}{
				"b": "second",
				"c": "third",
			},
		},
		"check unique custom": {
			input: `[{"v":"a"},{"v":"b"},{"v":"c"},{"v":"b"},{"v":"d"},{"v":"a"}].unique(v)`,
			output: []interface{}{
				map[string]interface{}{"v": "a"},
				map[string]interface{}{"v": "b"},
				map[string]interface{}{"v": "c"},
				map[string]interface{}{"v": "d"},
			},
		},
		"check unique bad": {
			input: `[{"v":"a"},{"v":"b"},{"v":"c"},{"v":"b"},{"v":"d"},{"v":"a"}].unique()`,
			err:   "index 0: expected string or number value, found object",
		},
		"check unique not array": {
			input: `"foo".unique()`,
			err:   "expected array value, found string: foo",
		},
		"check unique": {
			input:  `[3.0,5,3,4,5.1,5].unique()`,
			output: []interface{}{float64(3), int64(5), int64(4), float64(5.1)},
		},
		"check unique strings": {
			input:  `["a","b","c","b","d","a"].unique()`,
			output: []interface{}{"a", "b", "c", "d"},
		},
		"check unique mixed": {
			input:  `[3.0,"a","5",3,"b",5,"c","b",5.0,"d","a"].unique()`,
			output: []interface{}{float64(3), "a", "5", "b", int64(5), "c", "d"},
		},
		"check html escape query": {
			input:  `"foo & bar".escape_html()`,
			output: "foo &amp; bar",
		},
		"check html escape query bytes": {
			input: `content().escape_html()`,
			messages: []easyMsg{
				{content: `foo & bar`},
			},
			output: "foo &amp; bar",
		},
		"check html unescape query": {
			input:  `"foo &amp; bar".unescape_html()`,
			output: "foo & bar",
		},
		"check html unescape query bytes": {
			input: `content().unescape_html()`,
			messages: []easyMsg{
				{content: `foo &amp; bar`},
			},
			output: "foo & bar",
		},
		"check sort custom": {
			input:  `[3,22,13,7,30].sort(left > right)`,
			output: []interface{}{int64(30), int64(22), int64(13), int64(7), int64(3)},
		},
		"check sort strings custom": {
			input:  `["c","a","f","z"].sort(left > right)`,
			output: []interface{}{"z", "f", "c", "a"},
		},
		"check join": {
			input:  `["foo","bar"].join(",")`,
			output: "foo,bar",
		},
		"check join 2": {
			input:  `["foo"].join(",")`,
			output: "foo",
		},
		"check join 3": {
			input:  `[].join(",")`,
			output: "",
		},
		"check join no delim": {
			input:  `["foo","bar"].join()`,
			output: "foobar",
		},
		"check join fail not array": {
			input: `"foo".join(",")`,
			err:   "expected array value, found string: foo",
		},
		"check join fail number": {
			input: `["foo",10,"bar"].join(",")`,
			err:   "failed to join element 1: expected string value, found number: 10",
		},
		"check regexp find all submatch": {
			input: `"-axxb-ab-".re_find_all_submatch("a(x*)b")`,
			output: []interface{}{
				[]interface{}{"axxb", "xx"},
				[]interface{}{"ab", ""},
			},
		},
		"check regexp find all submatch bytes": {
			input:    `content().re_find_all_submatch("a(x*)b")`,
			messages: []easyMsg{{content: `-axxb-ab-`}},
			output: []interface{}{
				[]interface{}{"axxb", "xx"},
				[]interface{}{"ab", ""},
			},
		},
		"check regexp find all": {
			input:  `"paranormal".re_find_all("a.")`,
			output: []interface{}{"ar", "an", "al"},
		},
		"check regexp find all bytes": {
			input:    `content().re_find_all("a.")`,
			messages: []easyMsg{{content: `paranormal`}},
			output:   []interface{}{"ar", "an", "al"},
		},
		"check type": {
			input:  `"foobar".type()`,
			output: "string",
		},
		"check type 2": {
			input:  `match { true == true => "foobar", _ => false }.type()`,
			output: "string",
		},
		"check has_prefix": {
			input:    `"foobar".has_prefix("foo") && content().has_prefix("foo")`,
			messages: []easyMsg{{content: `foobar`}},
			output:   true,
		},
		"check has_prefix neg": {
			input:    `"foobar".has_prefix("bar") || content().has_prefix("bar")`,
			messages: []easyMsg{{content: `foobar`}},
			output:   false,
		},
		"check has_suffix": {
			input:    `"foobar".has_suffix("bar") && content().has_suffix("bar")`,
			messages: []easyMsg{{content: `foobar`}},
			output:   true,
		},
		"check has_suffix neg": {
			input:    `"foobar".has_suffix("foo") || content().has_suffix("foo")`,
			messages: []easyMsg{{content: `foobar`}},
			output:   false,
		},
		"check bool": {
			input:  `"true".bool()`,
			output: true,
		},
		"check bool 2": {
			input:  `"false".bool()`,
			output: false,
		},
		"check bool 3": {
			input:  `true.bool()`,
			output: true,
		},
		"check bool 4": {
			input:  `false.bool()`,
			output: false,
		},
		"check bool 5": {
			input:  `5.bool()`,
			output: true,
		},
		"check bool 6": {
			input:  `0.bool()`,
			output: false,
		},
		"check bool 7": {
			input: `"nope".bool()`,
			err:   `expected bool value, found string: nope`,
		},
		"check bool 8": {
			input:  `"nope".bool(true)`,
			output: true,
		},
		"check bool 9": {
			input:  `"nope".bool(false)`,
			output: false,
		},
		"check number": {
			input:  `"21".number()`,
			output: float64(21),
		},
		"check number 2": {
			input: `"nope".number()`,
			err:   `strconv.ParseFloat: parsing "nope": invalid syntax`,
		},
		"check number 3": {
			input:  `"nope".number(5)`,
			output: float64(5),
		},
		"check number 4": {
			input:  `"nope".number(5.2)`,
			output: float64(5.2),
		},
		"check index": {
			input:  `["foo","bar","baz"].index(1)`,
			output: "bar",
		},
		"check index neg": {
			input:  `["foo","bar","baz"].index(-1)`,
			output: "baz",
		},
		"check index oob": {
			input:  `["foo","bar","baz"].index(4).catch("buz")`,
			output: "buz",
		},
		"check index oob neg": {
			input:  `["foo","bar","baz"].index(-4).catch("buz")`,
			output: "buz",
		},
		"check url escape query": {
			input:  `"foo & bar".escape_url_query()`,
			output: "foo+%26+bar",
		},
		"check url escape query bytes": {
			input: `content().escape_url_query()`,
			messages: []easyMsg{
				{content: `foo & bar`},
			},
			output: "foo+%26+bar",
		},
		"check url unescape query": {
			input:  `"foo+%26+bar".unescape_url_query()`,
			output: "foo & bar",
		},
		"check url unescape query bytes": {
			input: `content().unescape_url_query()`,
			messages: []easyMsg{
				{content: `foo+%26+bar`},
			},
			output: "foo & bar",
		},
		"check flatten": {
			input: `json().flatten()`,
			messages: []easyMsg{
				{content: `["foo",["bar","baz"],"buz"]`},
			},
			output: []interface{}{
				"foo", "bar", "baz", "buz",
			},
		},
		"check flatten 2": {
			input: `json().flatten()`,
			messages: []easyMsg{
				{content: `[]`},
			},
			output: []interface{}{},
		},
		"check flatten 3": {
			input: `json().flatten()`,
			messages: []easyMsg{
				{content: `["foo","bar","baz","buz"]`},
			},
			output: []interface{}{
				"foo", "bar", "baz", "buz",
			},
		},
		"check collapse": {
			input: `json().collapse()`,
			messages: []easyMsg{
				{content: `{"foo":[{"bar":"1"},{"bar":"2"}]}`},
			},
			output: map[string]interface{}{
				"foo.0.bar": "1",
				"foo.1.bar": "2",
			},
		},
		"check sha1 hash": {
			input:  `"hello world".hash("sha1").encode("hex")`,
			output: `2aae6c35c94fcfb415dbe95f408b9ce91ee846ed`,
		},
		"check hmac sha1 hash": {
			input:  `"hello world".hash("hmac_sha1","static-key").encode("hex")`,
			output: `d87e5f068fa08fe90bb95bc7c8344cb809179d76`,
		},
		"check hmac sha1 hash 2": {
			input:  `"hello world".hash("hmac_sha1","foo").encode("hex")`,
			output: `20224529cc42a39bacc96459f6ead9d17da7f128`,
		},
		"check sha256 hash": {
			input:  `"hello world".hash("sha256").encode("hex")`,
			output: `b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9`,
		},
		"check hmac sha256 hash": {
			input:  `"hello world".hash("hmac_sha256","static-key").encode("hex")`,
			output: `b1cdce8b2add1f96135b2506f8ab748ae8ef15c49c0320357a6d168c42e20746`,
		},
		"check sha512 hash": {
			input:  `"hello world".hash("sha512").encode("hex")`,
			output: `309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f`,
		},
		"check hmac sha512 hash": {
			input:  `"hello world".hash("hmac_sha512","static-key").encode("hex")`,
			output: `fd5d5ed60b96e820ebaace4fed962a401adefd3e89c51a374f0bb7f49ed02892af8bc8591628dcbc8b5f065df6bb06588cba95d488c1c8b88faa7cbe08e4558d`,
		},
		"check xxhash64 hash": {
			input:  `"hello world".hash("xxhash64").string()`,
			output: `5020219685658847592`,
		},
		"check hex encode": {
			input:  `"hello world".encode("hex")`,
			output: `68656c6c6f20776f726c64`,
		},
		"check hex decode": {
			input:  `"68656c6c6f20776f726c64".decode("hex").string()`,
			output: `hello world`,
		},
		"check base64 encode": {
			input:  `"hello world".encode("base64")`,
			output: `aGVsbG8gd29ybGQ=`,
		},
		"check base64 decode": {
			input:  `"aGVsbG8gd29ybGQ=".decode("base64").string()`,
			output: `hello world`,
		},
		"check base64url encode": {
			input:  `"<<???>>".encode("base64url")`,
			output: `PDw_Pz8-Pg==`,
		},
		"check base64url decode": {
			input:  `"PDw_Pz8-Pg==".decode("base64url").string()`,
			output: `<<???>>`,
		},
		"check z85 encode": {
			input:  `"hello world!".encode("z85")`,
			output: `xK#0@zY<mxA+]nf`,
		},
		"check z85 decode": {
			input:  `"xK#0@zY<mxA+]nf".decode("z85").string()`,
			output: `hello world!`,
		},
		"check ascii85 encode": {
			input:  `"hello world!".encode("ascii85")`,
			output: `BOu!rD]j7BEbo80`,
		},
		"check ascii85 decode": {
			input:  `"BOu!rD]j7BEbo80".decode("ascii85").string()`,
			output: `hello world!`,
		},
		"check hex encode bytes": {
			input: `content().encode("hex")`,
			messages: []easyMsg{
				{content: `hello world`},
			},
			output: `68656c6c6f20776f726c64`,
		},
		"check strip html": {
			input:  `"<p>the plain <strong>old text</strong></p>".strip_html()`,
			output: `the plain old text`,
		},
		"check strip html bytes": {
			input: `content().strip_html()`,
			messages: []easyMsg{
				{content: `<p>the plain <strong>old text</strong></p>`},
			},
			output: []byte(`the plain old text`),
		},
		"check quote": {
			input: `this.quote()`,
			value: func() *interface{} {
				var s interface{} = linebreakStr
				return &s
			}(),
			output: `"foo\nbar\nbaz"`,
		},
		"check quote bytes": {
			input: `this.quote()`,
			value: func() *interface{} {
				var s interface{} = []byte(linebreakStr)
				return &s
			}(),
			output: `"foo\nbar\nbaz"`,
		},
		"check unquote": {
			input: `this.unquote()`,
			value: func() *interface{} {
				var s interface{} = "\"foo\\nbar\\nbaz\""
				return &s
			}(),
			output: linebreakStr,
		},
		"check unquote bytes": {
			input: `this.unquote()`,
			value: func() *interface{} {
				var s interface{} = []byte("\"foo\\nbar\\nbaz\"")
				return &s
			}(),
			output: linebreakStr,
		},
		"check replace": {
			input:  `"The foo ate my homework".replace("foo","dog")`,
			output: "The dog ate my homework",
		},
		"check replace bytes": {
			input: `content().replace("foo","dog")`,
			messages: []easyMsg{
				{content: `The foo ate my homework`},
			},
			output: []byte("The dog ate my homework"),
		},
		"check trim": {
			input:  `" the foo bar   ".trim()`,
			output: "the foo bar",
		},
		"check trim 2": {
			input:  `"!!?!the foo bar!".trim("!?")`,
			output: "the foo bar",
		},
		"check trim bytes": {
			input: `content().trim()`,
			messages: []easyMsg{
				{content: `  the foo bar  `},
			},
			output: []byte("the foo bar"),
		},
		"check trim bytes 2": {
			input: `content().trim("!?")`,
			messages: []easyMsg{
				{content: `!!?!the foo bar!`},
			},
			output: []byte("the foo bar"),
		},
		"check capitalize": {
			input:  `"the foo bar".capitalize()`,
			output: "The Foo Bar",
		},
		"check capitalize bytes": {
			input: `content().capitalize()`,
			messages: []easyMsg{
				{content: `the foo bar`},
			},
			output: []byte("The Foo Bar"),
		},
		"check split": {
			input:  `"foo,bar,baz".split(",")`,
			output: []interface{}{"foo", "bar", "baz"},
		},
		"check split bytes": {
			input: `content().split(",")`,
			messages: []easyMsg{
				{content: `foo,bar,baz,`},
			},
			output: []interface{}{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("")},
		},
		"check slice": {
			input:  `"foo bar baz".slice(0, 3)`,
			output: "foo",
		},
		"check slice 2": {
			input:  `"foo bar baz".slice(8)`,
			output: "baz",
		},
		"check slice neg start": {
			input:  `"foo bar baz".slice(-1)`,
			output: "z",
		},
		"check slice neg start 2": {
			input:  `"foo bar baz".slice(-2)`,
			output: "az",
		},
		"check slice neg start 3": {
			input:  `"foo bar baz".slice(-100)`,
			output: "foo bar baz",
		},
		"check slice neg end 1": {
			input:  `"foo bar baz".slice(0, -1)`,
			output: "foo bar ba",
		},
		"check slice neg end 2": {
			input:  `"foo bar baz".slice(0, -2)`,
			output: "foo bar b",
		},
		"check slice neg end 3": {
			input:  `"foo bar baz".slice(0, -100)`,
			output: "",
		},
		"check slice oob string": {
			input:  `"foo bar baz".slice(0, 30)`,
			output: "foo bar baz",
		},
		"check slice oob array": {
			input:  `["foo","bar","baz"].slice(0, 30)`,
			output: []interface{}{"foo", "bar", "baz"},
		},
		"check slice invalid": {
			input: `10.slice(8)`,
			err:   `expected array or string value, found number: 10`,
		},
		"check slice array": {
			input:  `["foo", "bar", "baz", "buz"].slice(1, 3)`,
			output: []interface{}{"bar", "baz"},
		},
		"check regexp match": {
			input:  `"there are 10 puppies".re_match("[0-9]")`,
			output: true,
		},
		"check regexp match 2": {
			input:  `"there are ten puppies".re_match("[0-9]")`,
			output: false,
		},
		"check regexp match dynamic": {
			input: `json("input").re_match(json("re"))`,
			messages: []easyMsg{
				{content: `{"input":"there are 10 puppies","re":"[0-9]"}`},
			},
			output: true,
		},
		"check regexp replace": {
			input:  `"foo ADD 70".re_replace("ADD ([0-9]+)","+($1)")`,
			output: "foo +(70)",
		},
		"check regexp replace dynamic": {
			input: `json("input").re_replace(json("re"), json("replace"))`,
			messages: []easyMsg{
				{content: `{"input":"foo ADD 70","re":"ADD ([0-9]+)","replace":"+($1)"}`},
			},
			output: "foo +(70)",
		},
		"check parse json": {
			input: `"{\"foo\":\"bar\"}".parse_json()`,
			output: map[string]interface{}{
				"foo": "bar",
			},
		},
		"check parse json invalid": {
			input: `"not valid json".parse_json()`,
			err:   `failed to parse value as JSON: invalid character 'o' in literal null (expecting 'u')`,
		},
		"check append": {
			input: `["foo"].append("bar","baz")`,
			output: []interface{}{
				"foo", "bar", "baz",
			},
		},
		"check append 2": {
			input: `["foo"].(this.append(this))`,
			output: []interface{}{
				"foo", []interface{}{"foo"},
			},
		},
		"check enumerated": {
			input: `["foo","bar","baz"].enumerated()`,
			output: []interface{}{
				map[string]interface{}{
					"index": int64(0),
					"value": "foo",
				},
				map[string]interface{}{
					"index": int64(1),
					"value": "bar",
				},
				map[string]interface{}{
					"index": int64(2),
					"value": "baz",
				},
			},
		},
		"check merge": {
			input: `{"foo":"val1"}.merge({"bar":"val2"})`,
			output: map[string]interface{}{
				"foo": "val1",
				"bar": "val2",
			},
		},
		"check merge 2": {
			input: `json().(foo.merge(bar))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":6},"foo":{"first":"val1","third":3}}`},
			},
			output: map[string]interface{}{
				"first":  "val1",
				"second": "val2",
				"third":  []interface{}{float64(3), float64(6)},
			},
		},
		"check merge 3": {
			input: `json().(this.merge(bar))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":6},"foo":{"first":"val1","third":3}}`},
			},
			output: map[string]interface{}{
				"foo": map[string]interface{}{
					"first": "val1",
					"third": float64(3),
				},
				"bar": map[string]interface{}{
					"second": "val2",
					"third":  float64(6),
				},
				"second": "val2",
				"third":  float64(6),
			},
		},
		"check merge 4": {
			input: `json().(foo.merge(bar))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":[6]},"foo":{"first":"val1","third":[3]}}`},
			},
			output: map[string]interface{}{
				"first":  "val1",
				"second": "val2",
				"third":  []interface{}{float64(3), float64(6)},
			},
		},
		"check merge 5": {
			input: `json().(foo.merge(bar).merge(foo))`,
			messages: []easyMsg{
				{content: `{"bar":{"second":"val2","third":[6]},"foo":{"first":"val1","third":[3]}}`},
			},
			output: map[string]interface{}{
				"first":  []interface{}{"val1", "val1"},
				"second": "val2",
				"third":  []interface{}{float64(3), float64(6), float64(3)},
			},
		},
		"check merge arrays": {
			input: `[].merge("foo")`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: []interface{}{"foo"},
		},
		"check merge arrays 2": {
			input: `["foo"].merge(["bar","baz"])`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: []interface{}{"foo", "bar", "baz"},
		},
		"check contains array": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `["nope","foo","bar"]`}},
			output:   true,
		},
		"check contains array 2": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `["nope","bar"]`}},
			output:   false,
		},
		"check contains array 3": {
			input: `json().contains(meta("against"))`,
			messages: []easyMsg{{
				content: `["nope","foo","bar"]`,
				meta:    map[string]string{"against": "bar"},
			}},
			output: true,
		},
		"check contains map": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `{"1":"nope","2":"foo","3":"bar"}`}},
			output:   true,
		},
		"check contains map 2": {
			input:    `json().contains("foo")`,
			messages: []easyMsg{{content: `{"1":"nope","3":"bar"}`}},
			output:   false,
		},
		"check contains invalid type": {
			input:    `json("nope").contains("foo")`,
			messages: []easyMsg{{content: `{"nope":false}`}},
			err:      "expected string, array or object value, found bool: false",
		},
		"check substr": {
			input:    `json("foo").contains("foo")`,
			messages: []easyMsg{{content: `{"foo":"hello foo world"}`}},
			output:   true,
		},
		"check substr 2": {
			input:    `json("foo").contains("foo")`,
			messages: []easyMsg{{content: `{"foo":"hello bar world"}`}},
			output:   false,
		},
		"check substr 3": {
			input: `json("foo").contains(meta("against"))`,
			messages: []easyMsg{{
				content: `{"foo":"nope foo bar"}`,
				meta:    map[string]string{"against": "bar"},
			}},
			output: true,
		},
		"check map each": {
			input:  `["foo","bar"].map_each(this.uppercase())`,
			output: []interface{}{"FOO", "BAR"},
		},
		"check map each 2": {
			input:  `["foo","bar"].map_each("(%v)".format(this).uppercase())`,
			output: []interface{}{"(FOO)", "(BAR)"},
		},
		"check fold": {
			input: `[3, 5, 2].fold(0, tally + value)`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: float64(10),
		},
		"check fold 2": {
			input: `["foo","bar"].fold("", "%v%v".format(tally, value))`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: "foobar",
		},
		"check fold 3": {
			input: `["foo","bar"].fold({"values":[]}, tally.merge({
				"values":[value]
			}))`,
			messages: []easyMsg{
				{content: `{}`},
			},
			output: map[string]interface{}{
				"values": []interface{}{"foo", "bar"},
			},
		},
		"check fold exec err": {
			input: `["foo","bar"].fold(this.does.not.exist, tally.merge({
				"values":[value]
			}))`,
			messages: []easyMsg{
				{content: `{}`},
			},
			err: "failed to extract tally initial value: context was undefined",
		},
		"check fold exec err 2": {
			input: `["foo","bar"].fold({"values":[]}, this.does.not.exist.number())`,
			messages: []easyMsg{
				{content: `{}`},
			},
			err: "expected number value, found null",
		},
		"check keys literal": {
			input:    `{"foo":1,"bar":2}.keys().sort()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{"bar", "foo"},
		},
		"check keys empty": {
			input:    `{}.keys()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{},
		},
		"check keys function": {
			input:    `json().keys().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			output:   []interface{}{"bar", "foo"},
		},
		"check keys error": {
			input:    `"foo".keys().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			err:      `expected object value, found string: foo`,
		},
		"check values literal": {
			input:    `{"foo":1,"bar":2}.values().sort()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{int64(1), int64(2)},
		},
		"check values empty": {
			input:    `{}.values()`,
			messages: []easyMsg{{content: `{}`}},
			output:   []interface{}{},
		},
		"check values function": {
			input:    `json().values().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			output:   []interface{}{1.0, 2.0},
		},
		"check values error": {
			input:    `"foo".values().sort()`,
			messages: []easyMsg{{content: `{"bar":2,"foo":1}`}},
			err:      `expected object value, found string: foo`,
		},
		"check aes-ctr encryption": {
			input:  `"hello world!".encrypt_aes("ctr","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff".decode("hex")).encode("hex")`,
			output: `84e9b31ff7400bdf80be7254`,
		},
		"check aes-ctr decryption": {
			input:  `"84e9b31ff7400bdf80be7254".decode("hex").decrypt_aes("ctr","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff".decode("hex")).string()`,
			output: `hello world!`,
		},
		"check aes-ofb encryption": {
			input:  `"hello world!".encrypt_aes("ofb","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"000102030405060708090a0b0c0d0e0f".decode("hex")).encode("hex")`,
			output: `389b0ba0f64d45d9a86553c8`,
		},
		"check aes-ofb decryption": {
			input:  `"389b0ba0f64d45d9a86553c8".decode("hex").decrypt_aes("ofb","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"000102030405060708090a0b0c0d0e0f".decode("hex")).string()`,
			output: `hello world!`,
		},
		"check aes-cbc encryption": {
			input:  `"6bc1bee22e409f96e93d7e117393172a".decode("hex").encrypt_aes("cbc","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"000102030405060708090a0b0c0d0e0f".decode("hex")).encode("hex")`,
			output: `7649abac8119b246cee98e9b12e9197d`,
		},
		"check aes-cbc encryption error": {
			input: `"hello world".encrypt_aes("cbc","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"000102030405060708090a0b0c0d0e0f".decode("hex")).encode("hex")`,
			err:   `plaintext is not a multiple of the block size`,
		},
		"check aes-cbc decryption": {
			input:  `"7649abac8119b246cee98e9b12e9197d".decode("hex").decrypt_aes("cbc","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"000102030405060708090a0b0c0d0e0f".decode("hex")).string().encode("hex")`,
			output: `6bc1bee22e409f96e93d7e117393172a`,
		},
		"check aes-cbc decryption error": {
			input: `"7649abac81".decode("hex").decrypt_aes("cbc","2b7e151628aed2a6abf7158809cf4f3c".decode("hex"),"000102030405060708090a0b0c0d0e0f".decode("hex")).string().encode("hex")`,
			err:   `ciphertext is not a multiple of the block size`,
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

			e, perr := tryParse(test.input, false)
			require.Nil(t, perr)

			for i := 0; i < 10; i++ {
				res, err := e.Exec(FunctionContext{
					Value:    test.value,
					Maps:     map[string]Function{},
					Index:    test.index,
					MsgBatch: msg,
				})
				if len(test.err) > 0 {
					require.EqualError(t, err, test.err)
				} else {
					require.NoError(t, err)
				}
				if !assert.Equal(t, test.output, res) {
					break
				}
			}

			// Ensure nothing changed
			for i, m := range test.messages {
				doc, err := msg.Get(i).JSON()
				if err == nil {
					msg.Get(i).SetJSON(doc)
				}
				assert.Equal(t, m.content, string(msg.Get(i).Get()))
			}
		})
	}
}
