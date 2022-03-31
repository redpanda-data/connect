// Package all imports all component implementations that ship with the open
// source Benthos repo. This is a convenient way of importing every single
// connector at the cost of a larger dependency tree for your application.
package all

import (
	// Import old legacy packages.
	_ "github.com/benthosdev/benthos/v4/public/components/legacy"

	// Import new service packages.
	_ "github.com/benthosdev/benthos/v4/internal/impl/amqp09"
	_ "github.com/benthosdev/benthos/v4/internal/impl/amqp1"
	_ "github.com/benthosdev/benthos/v4/internal/impl/aws"
	_ "github.com/benthosdev/benthos/v4/internal/impl/confluent"
	_ "github.com/benthosdev/benthos/v4/internal/impl/dgraph"
	_ "github.com/benthosdev/benthos/v4/internal/impl/gcp"
	_ "github.com/benthosdev/benthos/v4/internal/impl/generic"
	_ "github.com/benthosdev/benthos/v4/internal/impl/influxdb"
	_ "github.com/benthosdev/benthos/v4/internal/impl/jaeger"
	_ "github.com/benthosdev/benthos/v4/internal/impl/kafka"
	_ "github.com/benthosdev/benthos/v4/internal/impl/maxmind"
	_ "github.com/benthosdev/benthos/v4/internal/impl/memcached"
	_ "github.com/benthosdev/benthos/v4/internal/impl/mongodb"
	_ "github.com/benthosdev/benthos/v4/internal/impl/msgpack"
	_ "github.com/benthosdev/benthos/v4/internal/impl/nats"
	_ "github.com/benthosdev/benthos/v4/internal/impl/parquet"
	_ "github.com/benthosdev/benthos/v4/internal/impl/prometheus"
	_ "github.com/benthosdev/benthos/v4/internal/impl/redis"
	_ "github.com/benthosdev/benthos/v4/internal/impl/snowflake"
	_ "github.com/benthosdev/benthos/v4/internal/impl/sql"
	_ "github.com/benthosdev/benthos/v4/internal/impl/statsd"
	"github.com/benthosdev/benthos/v4/internal/template"

	// Import all (supported) sql drivers
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func init() {
	_, _ = template.InitTemplates()
}
