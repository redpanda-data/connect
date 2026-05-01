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

package html

import (
	"fmt"

	"github.com/microcosm-cc/bluemonday"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	stripHTMLSpec := bloblangv2.NewPluginSpec().
		Category("String Manipulation").
		Description(`Removes HTML tags from a string, returning only the text content. Useful for extracting plain text from HTML documents, sanitizing user input, or preparing content for text analysis. Optionally preserves specific HTML elements while stripping all others.`).
		Param(bloblangv2.NewAnyParam("preserve").Description("Optional array of HTML element names to preserve (e.g., [\"strong\", \"em\", \"a\"]). All other HTML tags will be removed.").Optional())

	bloblangv2.MustRegisterMethod(
		"strip_html", stripHTMLSpec,
		func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			p := bluemonday.NewPolicy()

			// preserve is Optional() so Get returns an error when omitted; the
			// only error path is the not-provided case, so it's safe to ignore.
			if v, err := args.Get("preserve"); err == nil && v != nil {
				tags, ok := v.([]any)
				if !ok {
					return nil, fmt.Errorf("expected an array for parameter \"preserve\", got %T", v)
				}
				tagStrs := make([]string, len(tags))
				for i, ele := range tags {
					var ok bool
					if tagStrs[i], ok = ele.(string); !ok {
						return nil, fmt.Errorf("invalid arg at index %v: expected string, got %T", i, ele)
					}
				}
				p = p.AllowElements(tagStrs...)
			}

			return func(v any) (any, error) {
				s, err := valueAsString(v)
				if err != nil {
					return nil, err
				}
				return p.Sanitize(s), nil
			}, nil
		},
	)
}

// valueAsString preserves V1's StringMethod behaviour, which coerced []byte to
// string. V2's StringMethod is strict and would otherwise reject bytes
// receivers — preserving the V1 contract avoids silent breakage of mappings
// that piped raw bytes into strip_html.
func valueAsString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	}
	return "", fmt.Errorf("expected string or bytes, got %T", v)
}
