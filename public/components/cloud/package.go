// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

// Package cloud imports all enterprise and FOSS component implementations that
// ship with Redpanda Connect in the cloud.
package cloud

import (
	// Only import a subset of components for execution.
	_ "github.com/redpanda-data/connect/v4/public/components/a2a"
	_ "github.com/redpanda-data/connect/v4/public/components/amqp09"
	_ "github.com/redpanda-data/connect/v4/public/components/avro"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/azure"
	_ "github.com/redpanda-data/connect/v4/public/components/changelog"
	_ "github.com/redpanda-data/connect/v4/public/components/cohere"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/crypto"
	_ "github.com/redpanda-data/connect/v4/public/components/cyborgdb"
	_ "github.com/redpanda-data/connect/v4/public/components/dgraph"
	_ "github.com/redpanda-data/connect/v4/public/components/elasticsearch/v8"
	_ "github.com/redpanda-data/connect/v4/public/components/elasticsearch/v9"
	_ "github.com/redpanda-data/connect/v4/public/components/gateway"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/git"
	_ "github.com/redpanda-data/connect/v4/public/components/google"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/jira"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/maxmind"
	_ "github.com/redpanda-data/connect/v4/public/components/memcached"
	_ "github.com/redpanda-data/connect/v4/public/components/mongodb"
	_ "github.com/redpanda-data/connect/v4/public/components/mongodb/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/mqtt"
	_ "github.com/redpanda-data/connect/v4/public/components/msgpack"
	_ "github.com/redpanda-data/connect/v4/public/components/mssqlserver"
	_ "github.com/redpanda-data/connect/v4/public/components/mysql"
	_ "github.com/redpanda-data/connect/v4/public/components/nats"
	_ "github.com/redpanda-data/connect/v4/public/components/openai"
	_ "github.com/redpanda-data/connect/v4/public/components/opensearch"
	_ "github.com/redpanda-data/connect/v4/public/components/oracledb"
	_ "github.com/redpanda-data/connect/v4/public/components/otlp"
	_ "github.com/redpanda-data/connect/v4/public/components/pinecone"
	_ "github.com/redpanda-data/connect/v4/public/components/postgresql"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/pure/extended"
	_ "github.com/redpanda-data/connect/v4/public/components/qdrant"
	_ "github.com/redpanda-data/connect/v4/public/components/questdb"
	_ "github.com/redpanda-data/connect/v4/public/components/redis"
	_ "github.com/redpanda-data/connect/v4/public/components/redpanda"
	_ "github.com/redpanda-data/connect/v4/public/components/sftp"
	_ "github.com/redpanda-data/connect/v4/public/components/slack"
	_ "github.com/redpanda-data/connect/v4/public/components/snowflake"
	_ "github.com/redpanda-data/connect/v4/public/components/spicedb"
	_ "github.com/redpanda-data/connect/v4/public/components/splunk"
	_ "github.com/redpanda-data/connect/v4/public/components/sql/base"
	_ "github.com/redpanda-data/connect/v4/public/components/text"
	_ "github.com/redpanda-data/connect/v4/public/components/tigerbeetle"
	_ "github.com/redpanda-data/connect/v4/public/components/timeplus"

	// Import all (supported) sql drivers.
	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"
)
