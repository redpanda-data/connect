package pure

import (
	"github.com/Jeffail/gabs/v2"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterMethodV2("squash",
		bloblang.NewPluginSpec().
			Category(query.MethodCategoryObjectAndArray).
			Description("Squashes an array of objects into a single object, where key collisions result in the values being merged (following similar rules as the `.merge()` method)").
			Example("", `root.locations = this.locations.map_each(loc -> {loc.state: [loc.name]}).squash()`,
				[2]string{
					`{"locations":[{"name":"Seattle","state":"WA"},{"name":"New York","state":"NY"},{"name":"Bellevue","state":"WA"},{"name":"Olympia","state":"WA"}]}`,
					`{"locations":{"NY":["New York"],"WA":["Seattle","Bellevue","Olympia"]}}`,
				},
			),
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.ArrayMethod(func(i []interface{}) (interface{}, error) {
				root := gabs.New()
				for _, v := range i {
					if err := root.Merge(gabs.Wrap(v)); err != nil {
						return nil, err
					}
				}
				return root.Data(), nil
			}), nil
		}); err != nil {
		panic(err)
	}
}
