package xml

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("parse_xml",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryParsing).
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
}
