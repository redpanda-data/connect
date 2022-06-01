package lang

import (
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {

	fakerSpec := bloblang.NewPluginSpec().
		Experimental().
		Category("String Manipulation").
		Description("Uses the values in the structure to map to functions in the faker library. If value doesn't match a faker method, it remains unchanged.").
		Param(bloblang.NewStringParam("function").Description("The name of the function to use to generate the string.")).
		Example("Sets email to a randomly generated email address.",
			`root.email = fake("email")`)

	if err := bloblang.RegisterFunctionV2(
		"fake", fakerSpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			functionKey, err := args.GetString("function")
			if err != nil {
				return nil, err
			}

			return func() (interface{}, error) {
				return GetFakeValue(functionKey), nil
			}, nil
		},
	); err != nil {
		panic(err)
	}

}
