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

package html

import (
	"fmt"

	"github.com/microcosm-cc/bluemonday"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	stripHTMLSpec := bloblang.NewPluginSpec().
		Category("String Manipulation").
		Description(`Removes HTML tags from a string, returning only the text content. Useful for extracting plain text from HTML documents, sanitizing user input, or preparing content for text analysis. Optionally preserves specific HTML elements while stripping all others.`).
		Example("Extract plain text from HTML content", `root.plain_text = this.html_content.strip_html()`,
			[2]string{
				`{"html_content":"<p>Welcome to <strong>Redpanda Connect</strong>!</p>"}`,
				`{"plain_text":"Welcome to Redpanda Connect!"}`,
			}).
		Example("Preserve specific HTML elements while removing others",
			`root.sanitized = this.html.strip_html(["strong", "em"])`,
			[2]string{
				`{"html":"<div><p>Some <strong>bold</strong> and <em>italic</em> text with a <script>alert('xss')</script></p></div>"}`,
				`{"sanitized":"Some <strong>bold</strong> and <em>italic</em> text with a "}`,
			}).
		Param(bloblang.NewAnyParam("preserve").Description("Optional array of HTML element names to preserve (e.g., [\"strong\", \"em\", \"a\"]). All other HTML tags will be removed.").Optional())

	if err := bloblang.RegisterMethodV2(
		"strip_html", stripHTMLSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			p := bluemonday.NewPolicy()

			var tags []any
			if rawArgs := args.AsSlice(); len(rawArgs) > 0 {
				tags, _ = rawArgs[0].([]any)
			}

			if len(tags) > 0 {
				tagStrs := make([]string, len(tags))
				for i, ele := range tags {
					var ok bool
					if tagStrs[i], ok = ele.(string); !ok {
						return nil, fmt.Errorf("invalid arg at index %v: expected string, got %T", i, ele)
					}
				}
				p = p.AllowElements(tagStrs...)
			}

			return bloblang.StringMethod(func(s string) (any, error) {
				return p.Sanitize(s), nil
			}), nil
		},
	); err != nil {
		panic(err)
	}
}
