// Copyright 2026 Redpanda Data, Inc.
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

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	bloblangv2.MustRegisterMethod("parse_xml",
		bloblangv2.NewPluginSpec().
			Category("Parsing").
			Description(`Parses an XML document into a structured object. Converts XML elements to JSON-like objects following these rules:

- Element attributes are prefixed with a hyphen (e.g., `+"`-id`"+` for an `+"`id`"+` attribute)
- Elements with both attributes and text content store the text in a `+"`#text`"+` field
- Repeated elements become arrays
- XML comments, directives, and processing instructions are ignored
- Optionally cast numeric and boolean strings to their proper types`).
			Param(bloblangv2.NewBoolParam("cast").
				Description("Whether to automatically cast numeric and boolean string values to their proper types. When false, all values remain as strings.").
				Default(false)),
		func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			cast, err := args.GetBool("cast")
			if err != nil {
				return nil, err
			}
			return func(v any) (any, error) {
				xmlBytes, err := valueAsBytes(v)
				if err != nil {
					return nil, err
				}
				xmlObj, err := ToMap(xmlBytes, cast)
				if err != nil {
					return nil, fmt.Errorf("parsing value as XML: %w", err)
				}
				return xmlObj, nil
			}, nil
		})

	bloblangv2.MustRegisterMethod("format_xml",
		bloblangv2.NewPluginSpec().
			Category("Parsing").
			Description(`Serializes an object into an XML document. Converts structured data to XML format with support for attributes (prefixed with hyphen), custom indentation, and configurable root element. Returns XML as a byte array.`).
			Param(bloblangv2.NewStringParam("indent").Description(
				"String to use for each level of indentation (default is 4 spaces). Each nested XML element will be indented by this string.").
				Default(strings.Repeat(" ", 4))).
			Param(bloblangv2.NewBoolParam("no_indent").Description(
				"Disable indentation and newlines to produce compact XML on a single line.").
				Default(false)).
			Param(bloblangv2.NewStringParam("root_tag").Description(
				"Custom name for the root XML element. By default, the root element name is derived from the first key in the object.").
				Default("")),
		func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			indent, err := args.GetString("indent")
			if err != nil {
				return nil, err
			}
			noIndent, err := args.GetBool("no_indent")
			if err != nil {
				return nil, err
			}
			rt, err := args.GetString("root_tag")
			if err != nil {
				return nil, err
			}
			return bloblangv2.ObjectMethod(func(obj map[string]any) (any, error) {
				if noIndent {
					return mxj.Map(obj).Xml()
				}
				var rootTag []string
				if rt != "" {
					rootTag = append(rootTag, rt)
				}
				return mxj.Map(obj).XmlIndent("", indent, rootTag...)
			}), nil
		})
}

// valueAsBytes mirrors V1's bloblang.ValueAsBytes coercion so parse_xml
// continues to accept both string and []byte receivers. V2's BytesMethod is
// strict and would reject string receivers — preserved here for parity with
// V1 behaviour.
func valueAsBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	}
	return nil, fmt.Errorf("expected string or bytes, got %T", v)
}
