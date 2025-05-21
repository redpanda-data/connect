// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xml

import (
	"fmt"
	"strings"

	"github.com/clbanning/mxj/v2"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("parse_xml",
		bloblang.NewPluginSpec().
			Category("Parsing").
			Description(`
Attempts to parse a string as an XML document and returns a structured result, where elements appear as keys of an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen, `+"`-`"+`, to the attribute label.
- If the element is a simple element and has attributes, the element value is given the key `+"`#text`"+`.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.
- If cast is true, try to cast values to numbers and booleans instead of returning strings.
`).
			Example("", `root.doc = this.doc.parse_xml()`, [2]string{
				`{"doc":"<root><title>This is a title</title><content>This is some content</content></root>"}`,
				`{"doc":{"root":{"content":"This is some content","title":"This is a title"}}}`,
			}).
			Example("", `root.doc = this.doc.parse_xml(cast: false)`, [2]string{
				`{"doc":"<root><title>This is a title</title><number id=99>123</number><bool>True</bool></root>"}`,
				`{"doc":{"root":{"bool":"True","number":{"#text":"123","-id":"99"},"title":"This is a title"}}}`,
			}).
			Example("", `root.doc = this.doc.parse_xml(cast: true)`, [2]string{
				`{"doc":"<root><title>This is a title</title><number id=99>123</number><bool>True</bool></root>"}`,
				`{"doc":{"root":{"bool":true,"number":{"#text":123,"-id":99},"title":"This is a title"}}}`,
			}).
			Param(bloblang.NewBoolParam("cast").
				Description("whether to try to cast values that are numbers and booleans to the right type.").
				Optional().Default(false)),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			castOpt, err := args.GetOptionalBool("cast")
			if err != nil {
				return nil, err
			}
			cast := false
			if castOpt != nil {
				cast = *castOpt
			}
			return bloblang.BytesMethod(func(xmlBytes []byte) (any, error) {
				xmlObj, err := ToMap(xmlBytes, cast)
				if err != nil {
					return nil, fmt.Errorf("failed to parse value as XML: %w", err)
				}
				return xmlObj, nil
			}), nil
		}); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_xml",
		bloblang.NewPluginSpec().
			Category("Parsing").
			Description(`
Serializes a target value into an XML byte array.
`).
			Example("Serializes a target value into a pretty-printed XML byte array (with 4 space indentation by default).",
				`root = this.format_xml()`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`<foo>
    <bar>
        <baz>foo bar baz</baz>
    </bar>
</foo>`,
				},
				[2]string{
					`{"-foo":"bar","fizz":"buzz"}`,
					`<doc foo="bar">
    <fizz>buzz</fizz>
</doc>`,
				},
				[2]string{
					`{"foo":[{"bar":"baz"},{"fizz":"buzz"}]}`,
					`<doc>
    <foo>
        <bar>baz</bar>
    </foo>
    <foo>
        <fizz>buzz</fizz>
    </foo>
</doc>`,
				},
			).
			Example("Pass a string to the `indent` parameter in order to customise the indentation.",
				`root = this.format_xml("  ")`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`<foo>
  <bar>
    <baz>foo bar baz</baz>
  </bar>
</foo>`,
				},
			).
			Example("Use the `.string()` method in order to coerce the result into a string.",
				`root.doc = this.format_xml("").string()`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`{"doc":"<foo>\n<bar>\n<baz>foo bar baz</baz>\n</bar>\n</foo>"}`,
				},
			).
			Example("Set the `no_indent` parameter to true to disable indentation.",
				`root = this.format_xml(no_indent: true)`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`<foo><bar><baz>foo bar baz</baz></bar></foo>`,
				},
			).
			Example("Set a custom root tag.",
				`root = this.format_xml(root_tag: "blobfish")`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`<blobfish>
    <foo>
        <bar>
            <baz>foo bar baz</baz>
        </bar>
    </foo>
</blobfish>`,
				},
				[2]string{
					`{"-foo":"bar","fizz":"buzz"}`,
					`<blobfish foo="bar">
    <fizz>buzz</fizz>
</blobfish>`,
				},
				[2]string{
					`{"foo":[{"bar":"baz"},{"fizz":"buzz"}]}`,
					`<blobfish>
    <foo>
        <bar>baz</bar>
    </foo>
    <foo>
        <fizz>buzz</fizz>
    </foo>
</blobfish>`,
				},
			).
			Param(bloblang.NewStringParam("indent").Description(
				"Indentation string. Each element in an XML object or array will begin on a new, indented line followed by one or more copies of indent according to the indentation nesting.").
				Default(strings.Repeat(" ", 4))).
			Param(bloblang.NewBoolParam("no_indent").Description(
				"Disable indentation.").
				Default(false)).
			Param(bloblang.NewStringParam("root_tag").Description(
				"Root tag. Set this if you wish to override the root tag of the document.").
				Optional()),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.ObjectMethod(func(obj map[string]any) (any, error) {
				indent := ""
				if indentOpt, err := args.GetOptionalString("indent"); err != nil {
					return nil, err
				} else if indentOpt != nil {
					indent = *indentOpt
				}
				noIndentOpt, err := args.GetOptionalBool("no_indent")
				if err != nil {
					return nil, err
				}
				if noIndentOpt != nil && *noIndentOpt {
					return mxj.Map(obj).Xml()
				}
				var rootTag []string
				if rt, err := args.GetOptionalString("root_tag"); err != nil {
					return nil, err
				} else if rt != nil {
					rootTag = append(rootTag, *rt)
				}
				return mxj.Map(obj).XmlIndent("", indent, rootTag...)
			}), nil
		}); err != nil {
		panic(err)
	}
}
