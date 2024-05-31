package html

import (
	"fmt"

	"github.com/microcosm-cc/bluemonday"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	stripHTMLSpec := bloblang.NewPluginSpec().
		Category("String Manipulation").
		Description(`Attempts to remove all HTML tags from a target string.`).
		Example("", `root.stripped = this.value.strip_html()`,
			[2]string{
				`{"value":"<p>the plain <strong>old text</strong></p>"}`,
				`{"stripped":"the plain old text"}`,
			}).
		Example("It's also possible to provide an explicit list of element types to preserve in the output.",
			`root.stripped = this.value.strip_html(["article"])`,
			[2]string{
				`{"value":"<article><p>the plain <strong>old text</strong></p></article>"}`,
				`{"stripped":"<article>the plain old text</article>"}`,
			}).
		Param(bloblang.NewAnyParam("preserve").Description("An optional array of element types to preserve in the output.").Optional())

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
