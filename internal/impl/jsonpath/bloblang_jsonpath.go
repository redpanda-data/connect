package jsonpath

import (
	"context"
	"fmt"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/generikvault/gvalstrings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("json_path",
		bloblang.NewPluginSpec().
			Experimental().
			Category(query.MethodCategoryObjectAndArray).
			Description("Executes the given JSONPath expression on an object or array and returns the result. The JSONPath expression syntax can be found at https://goessner.net/articles/JsonPath/. For more complex logic, you can use Gval expressions (https://github.com/PaesslerAG/gval).").
			Example("", `root.all_names = this.json_path("$..name")`, [2]string{
				`{"name":"alice","foo":{"name":"bob"}}`,
				`{"all_names":["alice","bob"]}`,
			}, [2]string{
				`{"thing":["this","bar",{"name":"alice"}]}`,
				`{"all_names":["alice"]}`,
			}).
			Example("", `root.text_objects = this.json_path("$.body[?(@.type=='text')]")`, [2]string{
				`{"body":[{"type":"image","id":"foo"},{"type":"text","id":"bar"}]}`,
				`{"text_objects":[{"type":"text","id":"bar"}]}`,
			}).
			Param(bloblang.NewStringParam("expression").Description("The JSONPath expression to execute.")),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			// jsonPathLanguage includes the full gval scripting language and the single quote extension
			jsonPathLanguage := gval.Full(jsonpath.Language(), gvalstrings.SingleQuoted())
			expressionStr, err := args.GetString("expression")
			if err != nil {
				return nil, err
			}
			eval, err := jsonPathLanguage.NewEvaluable(expressionStr)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate json path expression: %w", err)
			}
			return func(v interface{}) (interface{}, error) {
				return eval(context.Background(), v)
			}, nil
		}); err != nil {
		panic(err)
	}
}
