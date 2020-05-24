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
			err:   `function returned non-bool type: string`,
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
		"check slice invalid": {
			input: `10.slice(8)`,
			err:   `expected string or array value, received int64`,
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
			err:      "expected string, array or map target, found bool",
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
			err: "function returned non-numerical type: <nil>",
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
			err:      `expected map, found string`,
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
			err:      `expected map, found string`,
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

			e, err := tryParse(test.input, false)
			require.NoError(t, err)

			for i := 0; i < 10; i++ {
				res, err := e.Exec(FunctionContext{
					Value: test.value,
					Maps:  map[string]Function{},
					Index: test.index,
					Msg:   msg,
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
