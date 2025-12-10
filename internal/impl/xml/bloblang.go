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
			Description(`Parses an XML document into a structured object. Converts XML elements to JSON-like objects following these rules:

- Element attributes are prefixed with a hyphen (e.g., `+"`-id`"+` for an `+"`id`"+` attribute)
- Elements with both attributes and text content store the text in a `+"`#text`"+` field
- Repeated elements become arrays
- XML comments, directives, and processing instructions are ignored
- Optionally cast numeric and boolean strings to their proper types`).
			Example("Parse XML document into object structure", `root.doc = this.doc.parse_xml()`, [2]string{
				`{"doc":"<root><title>This is a title</title><content>This is some content</content></root>"}`,
				`{"doc":{"root":{"content":"This is some content","title":"This is a title"}}}`,
			}).
			Example("Parse XML with type casting enabled to convert strings to numbers and booleans", `root.doc = this.doc.parse_xml(cast: true)`, [2]string{
				`{"doc":"<root><title>This is a title</title><number id=\"99\">123</number><bool>True</bool></root>"}`,
				`{"doc":{"root":{"bool":true,"number":{"#text":123,"-id":99},"title":"This is a title"}}}`,
			}).
			Param(bloblang.NewBoolParam("cast").
				Description("Whether to automatically cast numeric and boolean string values to their proper types. When false, all values remain as strings.").
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
			Description(`Serializes an object into an XML document. Converts structured data to XML format with support for attributes (prefixed with hyphen), custom indentation, and configurable root element. Returns XML as a byte array.`).
			Example("Serialize object to pretty-printed XML with default indentation",
				`root = this.format_xml()`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`<foo>
    <bar>
        <baz>foo bar baz</baz>
    </bar>
</foo>`,
				},
			).
			Example("Create compact XML without indentation for smaller message size",
				`root = this.format_xml(no_indent: true)`,
				[2]string{
					`{"foo":{"bar":{"baz":"foo bar baz"}}}`,
					`<foo><bar><baz>foo bar baz</baz></bar></foo>`,
				},
			).
			Param(bloblang.NewStringParam("indent").Description(
				"String to use for each level of indentation (default is 4 spaces). Each nested XML element will be indented by this string.").
				Default(strings.Repeat(" ", 4))).
			Param(bloblang.NewBoolParam("no_indent").Description(
				"Disable indentation and newlines to produce compact XML on a single line.").
				Default(false)).
			Param(bloblang.NewStringParam("root_tag").Description(
				"Custom name for the root XML element. By default, the root element name is derived from the first key in the object.").
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
