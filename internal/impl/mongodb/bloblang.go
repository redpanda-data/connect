package mongodb

import (
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func init() {
	objectIdParseSpec := bloblang.NewPluginSpec().
		Beta().
		Category("Parsing").
		Description("Decodes a string or a date into a [MongoDB's ObjectId](https://www.mongodb.com/docs/manual/reference/method/ObjectId/).").
		Example("", `root._id = content().parse_object_id()`)

	if err := bloblang.RegisterMethodV2(
		"parse_object_id", objectIdParseSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				switch data := v.(type) {
				case string:
					return primitive.ObjectIDFromHex(data)
				case []byte:
					return primitive.ObjectIDFromHex(string(data))
				case time.Time:
					return primitive.NewObjectIDFromTimestamp(data), nil
				default:
					return nil, fmt.Errorf("invalid type to parse object id: %T", data)
				}

			}, nil
		},
	); err != nil {
		panic(err)
	}
}
