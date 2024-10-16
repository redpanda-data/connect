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
	_ "github.com/redpanda-data/connect/v4/public/components/amqp09"
	_ "github.com/redpanda-data/connect/v4/public/components/avro"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/aws/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/azure"
	_ "github.com/redpanda-data/connect/v4/public/components/changelog"
	_ "github.com/redpanda-data/connect/v4/public/components/cohere"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/crypto"
	_ "github.com/redpanda-data/connect/v4/public/components/dgraph"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/maxmind"
	_ "github.com/redpanda-data/connect/v4/public/components/memcached"
	_ "github.com/redpanda-data/connect/v4/public/components/mqtt"
	_ "github.com/redpanda-data/connect/v4/public/components/msgpack"
	_ "github.com/redpanda-data/connect/v4/public/components/nats"
	_ "github.com/redpanda-data/connect/v4/public/components/openai"
	_ "github.com/redpanda-data/connect/v4/public/components/opensearch"
	_ "github.com/redpanda-data/connect/v4/public/components/otlp"
	_ "github.com/redpanda-data/connect/v4/public/components/pinecone"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/pure/extended"
	_ "github.com/redpanda-data/connect/v4/public/components/qdrant"
	_ "github.com/redpanda-data/connect/v4/public/components/questdb"
	_ "github.com/redpanda-data/connect/v4/public/components/redis"
	_ "github.com/redpanda-data/connect/v4/public/components/sftp"
	_ "github.com/redpanda-data/connect/v4/public/components/snowflake"
	_ "github.com/redpanda-data/connect/v4/public/components/splunk"
	_ "github.com/redpanda-data/connect/v4/public/components/sql/base"

	// Import all (supported) sql drivers.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/sijms/go-ora/v2"
)
