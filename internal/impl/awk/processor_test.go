package awk

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func testAwk(confStr string, args ...any) (service.Processor, error) {
	pConf, err := awkSpec().ParseYAML(fmt.Sprintf(confStr, args...), nil)
	if err != nil {
		return nil, err
	}
	return newAWKProcFromConfig(pConf, service.MockResources())
}

func TestAWKValidation(t *testing.T) {
	a, err := testAwk(`
codec: json
program: "{ print foo_bar }"
`)
	require.NoError(t, err)

	_, err = a.Process(context.Background(), service.NewMessage([]byte("this is bad json")))
	require.Error(t, err)

	_, err = testAwk(`
codec: not valid
program: |
  {
    json_set("foo.bar", json_get("init.val"));
    json_set("foo.bar", json_get("foo.bar") " extra");
  }
`)
	require.Error(t, err)
}

func TestAWKBadExitStatus(t *testing.T) {
	a, err := testAwk(`
codec: none
program: "{ exit 1; print foo }"
`)
	require.NoError(t, err)

	_, err = a.Process(context.Background(), service.NewMessage([]byte("this will fail")))
	require.Error(t, err)
}

func TestAWKBadDateString(t *testing.T) {
	a, err := testAwk(`
codec: none
program: '{ print timestamp_unix("this isnt a date string") }'
`)
	require.NoError(t, err)

	_, err = a.Process(context.Background(), service.NewMessage([]byte("this is a value")))
	require.Error(t, err)
}

func TestAWK(t *testing.T) {
	type jTest struct {
		name          string
		metadata      map[string]string
		metadataAfter map[string]string
		codec         string
		program       string
		input         string
		output        string
		errContains   string
	}

	tests := []jTest{
		{
			name:    "no print 1",
			codec:   "none",
			program: `{ }`,
			input:   `hello world`,
			output:  ` `,
		},
		{
			name:    "empty print 1",
			codec:   "none",
			program: `{ print "" }`,
			input:   `hello world`,
			output:  ``,
		},
		{
			name: "metadata get 1",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			codec:   "none",
			program: `{ print metadata_get("meta.foo") }`,
			input:   `hello world`,
			output:  `12`,
		},
		{
			name: "metadata get 2",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			codec:   "none",
			program: `{ print metadata_get("meta.bar") }`,
			input:   `hello world`,
			output:  ``,
		},
		{
			name: "metadata set 1",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			metadataAfter: map[string]string{
				"meta.foo": "24",
				"meta.bar": "36",
			},
			codec:   "none",
			program: `{ metadata_set("meta.foo", 24); metadata_set("meta.bar", "36") }`,
			input:   `hello world`,
			output:  `hello world`,
		},
		{
			name:    "json get 1",
			codec:   "none",
			program: `{ print json_get("obj.foo") }`,
			input:   `{"obj":{"foo":12}}`,
			output:  `12`,
		},
		{
			name:    "json get 2",
			codec:   "none",
			program: `{ print json_get("obj.bar") }`,
			input:   `{"obj":{"foo":12}}`,
			output:  `null`,
		},
		{
			name:    "json get array 1",
			codec:   "none",
			program: `{ print json_get("obj.1.foo") }`,
			input:   `{"obj":[{"foo":11},{"foo":12}]}`,
			output:  `12`,
		},
		{
			name:    "json set array 1",
			codec:   "none",
			program: `{ json_set("obj.1.foo", "nope") }`,
			input:   `{"obj":[{"foo":11},{"foo":12}]}`,
			output:  `{"obj":[{"foo":11},{"foo":"nope"}]}`,
		},
		{
			name:        "json get 3",
			codec:       "none",
			program:     `{ print json_get("obj.bar") }`,
			input:       `not json content`,
			output:      `not json content`,
			errContains: "invalid character 'o' in literal null (expecting 'u')",
		},
		{
			name:    "json get 4",
			codec:   "none",
			program: `{ print json_get("obj.foo") }`,
			input:   `{"obj":{"foo":"hello"}}`,
			output:  `hello`,
		},
		{
			name:    "json set 1",
			codec:   "none",
			program: `{ json_set("obj.foo", "hello world") }`,
			input:   `{}`,
			output:  `{"obj":{"foo":"hello world"}}`,
		},
		{
			name:        "json set 2",
			codec:       "none",
			program:     `{ json_set("obj.foo", "hello world") }`,
			input:       `not json content`,
			output:      `not json content`,
			errContains: "invalid character 'o' in literal null (expecting 'u')",
		},
		{
			name:    "json delete 1",
			codec:   "none",
			program: `{ json_delete("obj.foo") }`,
			input:   `{"obj":{"foo":"hello world","bar":"baz"}}`,
			output:  `{"obj":{"bar":"baz"}}`,
		},
		{
			name:        "json delete 2",
			codec:       "none",
			program:     `{ json_delete("obj.foo") }`,
			input:       `not json content`,
			output:      `not json content`,
			errContains: "invalid character 'o' in literal null (expecting 'u')",
		},
		{
			name:    "json delete 3",
			codec:   "none",
			program: `{ json_delete("obj") }`,
			input:   `{"obj":{"foo":"hello world"}}`,
			output:  `{}`,
		},
		{
			name:  "json set, get and set again",
			codec: "none",
			program: `{
				 json_set("obj.foo", "hello world");
				 json_set("obj.foo", json_get("obj.foo") " 123");
			}`,
			input:  `{"obj":{"foo":"nope"}}`,
			output: `{"obj":{"foo":"hello world 123"}}`,
		},
		{
			name:    "json set int 1",
			codec:   "none",
			program: `{ json_set_int("obj.foo", 5) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":5}}`,
		},
		{
			name:    "json set float 1",
			codec:   "none",
			program: `{ json_set_float("obj.foo", 5.3) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":5.3}}`,
		},
		{
			name:    "json set bool 1",
			codec:   "none",
			program: `{ json_set_bool("obj.foo", "foo" == "foo") }`,
			input:   `{}`,
			output:  `{"obj":{"foo":true}}`,
		},
		{
			name: "metadata get 2",
			metadata: map[string]string{
				"meta.foo": "12",
			},
			codec:   "none",
			program: `{ print metadata_get("meta.bar") }`,
			input:   `hello world`,
			output:  ``,
		},
		{
			name:    "json 1",
			codec:   "json",
			program: `{ print obj_foo }`,
			input:   `{"obj":{"foo":"hello"}}`,
			output:  `hello`,
		},
		{
			name: "metadata 1",
			metadata: map[string]string{
				"meta.foo": "12",
				"meta.bar": "34",
			},
			codec:   "text",
			program: `{ print $2 " " meta_foo }`,
			input:   `hello world`,
			output:  `world 12`,
		},
		{
			name: "metadata plus json 1",
			metadata: map[string]string{
				"meta.foo": "12",
				"meta.bar": "34",
			},
			codec:   "json",
			program: `{ print obj_foo " " meta_foo }`,
			input:   `{"obj":{"foo":"hello"}}`,
			output:  `hello 12`,
		},
		{
			name:     "metadata not exist 1",
			metadata: map[string]string{},
			codec:    "none",
			program:  `{ print $2 meta_foo }`,
			input:    `foo`,
			output:   ``,
		},
		{
			name: "parse metadata datestring 1",
			metadata: map[string]string{
				"foostamp": "2018-12-18T11:57:32",
			},
			codec:   "text",
			program: `{ foo = foostamp; print timestamp_unix(foo) }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 2",
			metadata: map[string]string{
				"foostamp": "2018TOTALLY12CUSTOM18T11:57:32",
			},
			codec:   "text",
			program: `{ foo = foostamp; print timestamp_unix(foo, "2006TOTALLY01CUSTOM02T15:04:05") }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "parse metadata datestring 3",
			metadata: map[string]string{
				"foostamp": "2018-12-18T11:57:32",
			},
			codec:   "text",
			program: `{ print timestamp_unix(foostamp) }`,
			input:   `foo`,
			output:  `1545134252`,
		},
		{
			name: "format metadata unix custom 1",
			metadata: map[string]string{
				"foostamp": "1545134252",
			},
			codec:   "text",
			program: `{ print timestamp_format(foostamp, "02 Jan 06 15:04") }`,
			input:   `foo`,
			output:  `18 Dec 18 11:57`,
		},
		{
			name: "format metadata unix nano custom 1",
			metadata: map[string]string{
				"foostamp": "1545134252123000064",
			},
			codec:   "text",
			program: `{ print timestamp_format_nano(foostamp, "02 Jan 06 15:04:05.000000000") }`,
			input:   `foo`,
			output:  `18 Dec 18 11:57:32.123000064`,
		},
		{
			name:    "create json object 1",
			codec:   "none",
			program: `{ print create_json_object("foo", "1", "bar", "2", "baz", "3") }`,
			input:   `this is ignored`,
			output:  `{"bar":"2","baz":"3","foo":"1"}`,
		},
		{
			name:    "create json object 2",
			codec:   "none",
			program: `{ print create_json_object("foo", "1", "bar", 2, "baz", "true") }`,
			input:   `this is ignored`,
			output:  `{"bar":"2","baz":"true","foo":"1"}`,
		},
		{
			name:    "create json object 3",
			codec:   "none",
			program: `{ print create_json_object() }`,
			input:   `this is ignored`,
			output:  `{}`,
		},
		{
			name:    "create json array 1",
			codec:   "none",
			program: `{ print create_json_array("1", 2, "3") }`,
			input:   `this is ignored`,
			output:  `["1","2","3"]`,
		},
		{
			name:    "create json array 2",
			codec:   "none",
			program: `{ print create_json_array() }`,
			input:   `this is ignored`,
			output:  `[]`,
		},
		{
			name:    "json array append 1",
			codec:   "none",
			program: `{ json_append("obj.foo", "hello world") }`,
			input:   `{}`,
			output:  `{"obj":{"foo":["hello world"]}}`,
		},
		{
			name:    "json array append 2",
			codec:   "none",
			program: `{ json_append("obj.foo", "hello world") }`,
			input:   `{"0":"test"}`,
			output:  `{"0":"test","obj":{"foo":["hello world"]}}`,
		},
		{
			name:    "json array append 3",
			codec:   "none",
			program: `{ json_append("obj.foo", "hello world") }`,
			input:   `{"0":"test","obj":{"1":"test2"}}`,
			output:  `{"0":"test","obj":{"1":"test2","foo":["hello world"]}}`,
		},
		{
			name:    "json array append 4",
			codec:   "none",
			program: `{ json_append("obj.foo", "hello world") }`,
			input:   `{"obj":{"foo":"first"}}`,
			output:  `{"obj":{"foo":["first","hello world"]}}`,
		},
		{
			name:    "json array append 5",
			codec:   "none",
			program: `{ json_append("obj.foo", "hello world") }`,
			input:   `{"obj":{"foo":["first",2]}}`,
			output:  `{"obj":{"foo":["first",2,"hello world"]}}`,
		},
		{
			name:    "json array append int 1",
			codec:   "none",
			program: `{ json_append_int("obj.foo", 1) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":[1]}}`,
		},
		{
			name:    "json array append float 1",
			codec:   "none",
			program: `{ json_append_float("obj.foo", 1.2) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":[1.2]}}`,
		},
		{
			name:    "json array append bool 1",
			codec:   "none",
			program: `{ json_append_bool("obj.foo", 1) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":[true]}}`,
		},
		{
			name:    "json array append bool 0",
			codec:   "none",
			program: `{ json_append_bool("obj.foo", 0) }`,
			input:   `{}`,
			output:  `{"obj":{"foo":[false]}}`,
		},
		{
			name:    "json type 1",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{}`,
			output:  `undefined`,
		},
		{
			name:    "json type 2",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{"foo":null}`,
			output:  `null`,
		},
		{
			name:    "json type 3",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{"foo":5}`,
			output:  `float`,
		},
		{
			name:    "json type 4",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{"foo":"foo"}`,
			output:  `string`,
		},
		{
			name:    "json type 5",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{"foo":["foo",5,false]}`,
			output:  `array`,
		},
		{
			name:    "json type 6",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{"foo":false}`,
			output:  `bool`,
		},
		{
			name:    "json type 7",
			codec:   "none",
			program: `{ print json_type("foo") }`,
			input:   `{"foo":{"foo":"bar"}}`,
			output:  `object`,
		},
		{
			name:    "json length 1",
			codec:   "none",
			program: `{ print json_length("foo") }`,
			input:   `{}`,
			output:  `0`,
		},
		{
			name:    "json length 2",
			codec:   "none",
			program: `{ print json_length("foo") }`,
			input:   `{"foo":5}`,
			output:  `0`,
		},
		{
			name:    "json length 3",
			codec:   "none",
			program: `{ print json_length("foo") }`,
			input:   `{"foo":[]}`,
			output:  `0`,
		},
		{
			name:    "json length 4",
			codec:   "none",
			program: `{ print json_length("foo") }`,
			input:   `{"foo":[1, 2, "three"]}`,
			output:  `3`,
		},
		{
			name:    "json length 5",
			codec:   "none",
			program: `{ print json_length("foo") }`,
			input:   `{"foo":"four"}`,
			output:  `4`,
		},
		{
			name:    "json length 6",
			codec:   "none",
			program: `{ print json_length("foo") }`,
			input:   `{"foo":""}`,
			output:  `0`,
		},
		{
			name:    "base64_encode",
			codec:   "none",
			program: `{ print base64_encode("blobs are cool") }`,
			output:  "YmxvYnMgYXJlIGNvb2w=",
		},
		{
			name:    "base64_decode succeeds",
			codec:   "none",
			program: `{ print base64_decode("YmxvYnMgYXJlIGNvb2w=") }`,
			output:  "blobs are cool",
		},
		{
			name:        "base64_decode fails on invalid input",
			codec:       "none",
			program:     `{ print base64_decode("$$^^**") }`,
			errContains: "illegal base64 data at input byte 0",
		},
	}

	for _, test := range tests {
		a, err := testAwk(`
codec: %v
program: %v
`, test.codec, strconv.Quote(test.program))
		require.NoError(t, err)

		msg := service.NewMessage([]byte(test.input))
		for k, v := range test.metadata {
			msg.MetaSetMut(k, v)
		}

		msgs, err := a.Process(context.Background(), msg)
		if err != nil {
			if test.errContains != "" {
				assert.ErrorContains(t, err, test.errContains, "Test '%s' failed", test.name)
			} else {
				assert.NoError(t, err, "Test '%s' failed", test.name)
			}
			return
		}
		require.Len(t, msgs, 1)

		if exp := test.metadataAfter; len(exp) > 0 {
			act := map[string]string{}
			_ = msgs[0].MetaWalk(func(k, v string) error {
				act[k] = v
				return nil
			})
			if !reflect.DeepEqual(exp, act) {
				t.Errorf("Wrong metadata contents: %v != %v", act, exp)
			}
		}

		mBytes, err := msgs[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, string(mBytes), test.output)
	}
}
