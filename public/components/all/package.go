// Package all imports all component implementations that ship with the open
// source Benthos repo. This is a convenient way of importing every single
// connector at the cost of a larger dependency tree for your application.
package all

import (
	// Import old legacy packages.
	_ "github.com/Jeffail/benthos/v3/public/components/legacy"

	// Import new service packages.
	_ "github.com/Jeffail/benthos/v3/internal/impl/aws"
	_ "github.com/Jeffail/benthos/v3/internal/impl/confluent"
	_ "github.com/Jeffail/benthos/v3/internal/impl/gcp"
	_ "github.com/Jeffail/benthos/v3/internal/impl/generic"
	_ "github.com/Jeffail/benthos/v3/internal/impl/kafka"
	_ "github.com/Jeffail/benthos/v3/internal/impl/maxmind"
	_ "github.com/Jeffail/benthos/v3/internal/impl/memcached"
	_ "github.com/Jeffail/benthos/v3/internal/impl/mongodb"
	_ "github.com/Jeffail/benthos/v3/internal/impl/msgpack"
	_ "github.com/Jeffail/benthos/v3/internal/impl/nats"
	_ "github.com/Jeffail/benthos/v3/internal/impl/parquet"
	_ "github.com/Jeffail/benthos/v3/internal/impl/pulsar"
	_ "github.com/Jeffail/benthos/v3/internal/impl/sql"
	"github.com/Jeffail/benthos/v3/internal/template"

	// Import all (supported) sql drivers
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func init() {
	_, _ = template.InitTemplates()
}
