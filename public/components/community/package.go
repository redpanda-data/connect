// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package community imports all FOSS component implementations that ship with
// Redpanda Connect. This is a convenient way of importing every single
// connector at the cost of a larger dependency tree for your application.
package community

import (
	// Import all public sub-categories.
	_ "github.com/redpanda-data/connect/v4/public/components/amqp09"
	_ "github.com/redpanda-data/connect/v4/public/components/amqp1"
	_ "github.com/redpanda-data/connect/v4/public/components/avro"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/azure"
	_ "github.com/redpanda-data/connect/v4/public/components/beanstalkd"
	_ "github.com/redpanda-data/connect/v4/public/components/cassandra"
	_ "github.com/redpanda-data/connect/v4/public/components/changelog"
	_ "github.com/redpanda-data/connect/v4/public/components/cockroachdb"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
	_ "github.com/redpanda-data/connect/v4/public/components/couchbase"
	_ "github.com/redpanda-data/connect/v4/public/components/crypto"
	_ "github.com/redpanda-data/connect/v4/public/components/cypher"
	_ "github.com/redpanda-data/connect/v4/public/components/dgraph"
	_ "github.com/redpanda-data/connect/v4/public/components/discord"
	_ "github.com/redpanda-data/connect/v4/public/components/elasticsearch/v8"
	_ "github.com/redpanda-data/connect/v4/public/components/gcp"
	_ "github.com/redpanda-data/connect/v4/public/components/git"
	_ "github.com/redpanda-data/connect/v4/public/components/hdfs"
	_ "github.com/redpanda-data/connect/v4/public/components/influxdb"
	_ "github.com/redpanda-data/connect/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/jaeger"
	_ "github.com/redpanda-data/connect/v4/public/components/javascript"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/maxmind"
	_ "github.com/redpanda-data/connect/v4/public/components/memcached"
	_ "github.com/redpanda-data/connect/v4/public/components/mongodb"
	_ "github.com/redpanda-data/connect/v4/public/components/mqtt"
	_ "github.com/redpanda-data/connect/v4/public/components/msgpack"
	_ "github.com/redpanda-data/connect/v4/public/components/nanomsg"
	_ "github.com/redpanda-data/connect/v4/public/components/nats"
	_ "github.com/redpanda-data/connect/v4/public/components/nsq"
	_ "github.com/redpanda-data/connect/v4/public/components/ockam"
	_ "github.com/redpanda-data/connect/v4/public/components/opensearch"
	_ "github.com/redpanda-data/connect/v4/public/components/otlp"
	_ "github.com/redpanda-data/connect/v4/public/components/pinecone"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
	_ "github.com/redpanda-data/connect/v4/public/components/pulsar"
	_ "github.com/redpanda-data/connect/v4/public/components/pure"
	_ "github.com/redpanda-data/connect/v4/public/components/pure/extended"
	_ "github.com/redpanda-data/connect/v4/public/components/pusher"
	_ "github.com/redpanda-data/connect/v4/public/components/qdrant"
	_ "github.com/redpanda-data/connect/v4/public/components/questdb"
	_ "github.com/redpanda-data/connect/v4/public/components/redis"
	_ "github.com/redpanda-data/connect/v4/public/components/redpanda"
	_ "github.com/redpanda-data/connect/v4/public/components/sentry"
	_ "github.com/redpanda-data/connect/v4/public/components/sftp"
	_ "github.com/redpanda-data/connect/v4/public/components/sideeffect"
	_ "github.com/redpanda-data/connect/v4/public/components/spicedb"
	_ "github.com/redpanda-data/connect/v4/public/components/sql"
	_ "github.com/redpanda-data/connect/v4/public/components/statsd"
	_ "github.com/redpanda-data/connect/v4/public/components/text"
	_ "github.com/redpanda-data/connect/v4/public/components/timeplus"
	_ "github.com/redpanda-data/connect/v4/public/components/twitter"
	_ "github.com/redpanda-data/connect/v4/public/components/wasm"
	_ "github.com/redpanda-data/connect/v4/public/components/zeromq"
)
