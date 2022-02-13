package mongodb

import (
	"context"
	"testing"

	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/stretchr/testify/require"
)

func TestSQLSelectInputEmptyShutdown(t *testing.T) {
	conf := `
dsn: "mongodb://foouser:foopass@localhost"
database: "foo"
collection: "bar"
query: |
  root.from = {"$lte": timestamp_unix()}
  root.to = {"$gte": timestamp_unix()}
`

	spec := mongoConfigSpec()
	env := service.NewEnvironment()

	mongoConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newMongoInput(mongoConfig)
	require.NoError(t, err)
	require.NoError(t, selectInput.Close(context.Background()))
}
