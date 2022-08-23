// Package all imports all component implementations that ship with the open
// source Benthos repo. This is a convenient way of importing every single
// connector at the cost of a larger dependency tree for your application.
package all

import (
	// Import new service packages.
	_ "github.com/benthosdev/benthos/v4/internal/impl/jsonpath"
	_ "github.com/benthosdev/benthos/v4/internal/impl/prometheus"
	_ "github.com/benthosdev/benthos/v4/internal/impl/pusher"
	_ "github.com/benthosdev/benthos/v4/internal/impl/redis"
	_ "github.com/benthosdev/benthos/v4/internal/impl/sftp"
	_ "github.com/benthosdev/benthos/v4/internal/impl/snowflake"
	_ "github.com/benthosdev/benthos/v4/internal/impl/sql"
	_ "github.com/benthosdev/benthos/v4/internal/impl/statsd"

	// Eventually `all` will consist only of public sub-categories
	_ "github.com/benthosdev/benthos/v4/public/components/amqp09"
	_ "github.com/benthosdev/benthos/v4/public/components/amqp1"
	_ "github.com/benthosdev/benthos/v4/public/components/avro"
	_ "github.com/benthosdev/benthos/v4/public/components/awk"
	_ "github.com/benthosdev/benthos/v4/public/components/aws"
	_ "github.com/benthosdev/benthos/v4/public/components/azure"
	_ "github.com/benthosdev/benthos/v4/public/components/cassandra"
	_ "github.com/benthosdev/benthos/v4/public/components/confluent"
	_ "github.com/benthosdev/benthos/v4/public/components/dgraph"
	_ "github.com/benthosdev/benthos/v4/public/components/elasticsearch"
	_ "github.com/benthosdev/benthos/v4/public/components/gcp"
	_ "github.com/benthosdev/benthos/v4/public/components/hdfs"
	_ "github.com/benthosdev/benthos/v4/public/components/influxdb"
	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/jaeger"
	_ "github.com/benthosdev/benthos/v4/public/components/kafka"
	_ "github.com/benthosdev/benthos/v4/public/components/maxmind"
	_ "github.com/benthosdev/benthos/v4/public/components/memcached"
	_ "github.com/benthosdev/benthos/v4/public/components/mongodb"
	_ "github.com/benthosdev/benthos/v4/public/components/mqtt"
	_ "github.com/benthosdev/benthos/v4/public/components/nanomsg"
	_ "github.com/benthosdev/benthos/v4/public/components/nats"
	_ "github.com/benthosdev/benthos/v4/public/components/nsq"
	_ "github.com/benthosdev/benthos/v4/public/components/otlp"
	_ "github.com/benthosdev/benthos/v4/public/components/parquet"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	_ "github.com/benthosdev/benthos/v4/public/components/pure/extended"

	// Import all (supported) sql drivers
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)
