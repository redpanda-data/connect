package mongodb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestMappings(t *testing.T) {
	testObjectId, _ := primitive.ObjectIDFromHex("637452307599bdc8edbc367a")

	tests := []struct {
		name              string
		mapping           string
		output            any
		execErrorContains string
	}{
		{
			name:              "parse_object_id invalid string",
			mapping:           `root = "invalid object id".parse_object_id()`,
			execErrorContains: `the provided hex string is not a valid ObjectID`,
		},
		{
			name:    "parse_object_id valid string",
			mapping: `root = "` + testObjectId.Hex() + `".parse_object_id()`,
			output:  testObjectId,
		}, {
			name:    "parse_object_id valid date",
			mapping: `root = "2022-11-16T03:00:00Z".ts_parse("2006-01-02T15:04:05Z").parse_object_id().ts_format("2006-01-02T15:04:05Z")`,
			output:  "2022-11-16T03:00:00Z",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			m, err := bloblang.Parse(test.mapping)

			require.NoError(t, err)
			v, err := m.Query(nil)
			if test.execErrorContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.execErrorContains)
			} else {
				require.NoError(t, err)
				fmt.Println(test.output)
				fmt.Println(v)
				assert.Equal(t, test.output, v)
			}
		})
	}
}
