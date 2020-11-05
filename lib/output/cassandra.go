package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCassandra] = TypeSpec{
		constructor: NewCassandra,
		Summary: `
Send messages to a Cassandra database.`,
		Description: `
This output will send messages to a Cassandra database using the INSERT JSON functionality
provided by the database (https://cassandra.apache.org/doc/latest/cql/json.html#insert-json).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldAdvanced(
				"nodes",
				"A list of Cassandra nodes to connect to.",
				[]string{"localhost:9042"}),
			docs.FieldAdvanced(
				"password_authenticator",
				"An object containing the username and password.",
				writer.PasswordAuthenticator{Enabled: true, Username: "cassandra", Password: "cassandra"}),
			docs.FieldAdvanced(
				"keyspace",
				"The name of the Cassandra keyspace to use.",
				"benthos"),
			docs.FieldAdvanced(
				"table",
				"The name of the Cassandra table to use.",
				"benthos"),
			docs.FieldAdvanced(
				"consistency",
				"The consistency level to use.",
				"ANY", "ONE", "TWO", "THREE", "QUORUM", "ALL", "LOCAL_QUORUM", "EACH_QUORUM", "LOCAL_ONE"),
			docs.FieldAdvanced(
				"async",
				"A flag to determine whether inserts will be performed concurrently.",
				true, false),
			docs.FieldAdvanced(
				"backoff",
				"The mechanism used to provided retries at increasing intervals.",
				retries.Backoff{InitialInterval: "1s", MaxInterval: "5s", MaxElapsedTime: "30s"}),
			docs.FieldAdvanced(
				"max_retries",
				"The maximum number of retries to attempt.",
				10),
		},
	}
}

//------------------------------------------------------------------------------

// NewCassandra creates a new Cassandra output type.
func NewCassandra(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	c, err := writer.NewCassandra(conf.Cassandra, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		TypeCassandra, c, log, stats,
	)
}

//------------------------------------------------------------------------------
