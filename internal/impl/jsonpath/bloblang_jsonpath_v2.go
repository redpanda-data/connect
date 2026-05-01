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

package jsonpath

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	bloblangv2.MustRegisterMethod("json_path",
		bloblangv2.NewPluginSpec().
			Category("Object & Array Manipulation").
			Description("Executes the given JSONPath expression on an object or array and returns the result. The JSONPath expression syntax can be found at https://goessner.net/articles/JsonPath/. For more complex logic, you can use Gval expressions (https://github.com/PaesslerAG/gval).").
			Param(bloblangv2.NewStringParam("expression").Description("The JSONPath expression to execute.")),
		func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			expressionStr, err := args.GetString("expression")
			if err != nil {
				return nil, err
			}
			eval, err := jsonPathLanguage.NewEvaluable(expressionStr)
			if err != nil {
				return nil, fmt.Errorf("evaluating json path expression: %w", err)
			}
			return func(v any) (any, error) {
				return eval(context.Background(), v)
			}, nil
		})
}
