package crypto

import (
	"crypto/subtle"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func registerConstantTimeCompareMethod() error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryStrings).
		Description("Checks whether a string matches another string. The time it takes this method is a function of the length of the strings and is independent of the contents. If the lengths of the strings do not match it returns false immediately.").
		Param(bloblang.NewStringParam("target").Description("The string to compare to.")).
		Example("", `root.match = this.secret.constant_time_compare("there-are-many-blobs-in-the-sea")`, [2]string{
			`{"secret":"there-are-many-blobs-in-the-sea"}`,
			`{"match":true}`,
		}).
		Example("", `root.match = this.secret.constant_time_compare("will-i-ever-find-love")`, [2]string{
			`{"secret":"there-are-many-blobs-in-the-sea"}`,
			`{"match":false}`,
		})

	return bloblang.RegisterMethodV2("constant_time_compare", spec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		target, err := args.GetString("target")
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(source string) (interface{}, error) {
			a := []byte(source)
			b := []byte(target)

			if res := subtle.ConstantTimeCompare(a, b); res == 1 {
				return true, nil
			}

			return false, nil
		}), nil
	})
}

func init() {
	if err := registerConstantTimeCompareMethod(); err != nil {
		panic(err)
	}
}
