module github.com/benthosdev/benthos/v4

require (
	cloud.google.com/go/bigquery v1.26.0
	cloud.google.com/go/iam v0.1.0 // indirect
	cloud.google.com/go/pubsub v1.17.1
	cloud.google.com/go/storage v1.18.2
	cuelang.org/go v0.4.2
	github.com/AthenZ/athenz v1.10.43 // indirect
	github.com/Azure/azure-sdk-for-go v61.1.0+incompatible
	github.com/Azure/azure-sdk-for-go/sdk/azcore v0.22.0
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v0.6.0
	github.com/Azure/azure-sdk-for-go/sdk/internal v0.9.2 // indirect
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.17.0
	github.com/Azure/go-autorest/autorest v0.11.23
	github.com/Azure/go-autorest/autorest/adal v0.9.18 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/ClickHouse/clickhouse-go/v2 v2.2.0
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.4.0
	github.com/Jeffail/gabs/v2 v2.6.1
	github.com/Jeffail/grok v1.1.0
	github.com/Masterminds/squirrel v1.5.2
	github.com/OneOfOne/xxhash v1.2.8
	github.com/PaesslerAG/gval v1.2.0
	github.com/PaesslerAG/jsonpath v0.1.1
	github.com/Shopify/sarama v1.30.1
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/apache/pulsar-client-go v0.8.1
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20220524063205-c41616b2f512 // indirect
	github.com/apache/thrift v0.15.0 // indirect
	github.com/armon/go-metrics v0.3.4 // indirect
	github.com/aws/aws-lambda-go v1.28.0
	github.com/aws/aws-sdk-go v1.44.45
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.9.1 // indirect
	github.com/benhoyt/goawk v1.17.1
	github.com/bradfitz/gomemcache v0.0.0-20220106215444-fb4bf637b56d
	github.com/bwmarrin/snowflake v0.3.0
	github.com/bxcodec/faker/v3 v3.8.0
	github.com/cenkalti/backoff/v4 v4.1.3
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/clbanning/mxj/v2 v2.5.5
	github.com/colinmarc/hdfs v1.1.3
	github.com/containerd/continuity v0.2.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.1 // indirect
	github.com/denisenkom/go-mssqldb v0.11.0
	github.com/dgraph-io/ristretto v0.1.0
	github.com/docker/cli v20.10.12+incompatible // indirect
	github.com/docker/docker v20.10.12+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0
	github.com/eclipse/paho.mqtt.golang v1.3.5
	github.com/fatih/color v1.13.0
	github.com/fsnotify/fsnotify v1.5.1
	github.com/generikvault/gvalstrings v0.0.0-20180926130504-471f38f0112a
	github.com/go-redis/redis/v7 v7.4.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/gocql/gocql v0.0.0-20211222173705-d73e6b1002a7
	github.com/gofrs/uuid v4.2.0+incompatible
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/flatbuffers v2.0.5+incompatible // indirect
	github.com/google/go-cmp v0.5.8
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/gosimple/slug v1.12.0
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/influxdata/go-syslog/v3 v3.0.0
	github.com/influxdata/influxdb1-client v0.0.0-20200827194710-b269163b24ab
	github.com/itchyny/gojq v0.12.6
	github.com/itchyny/timefmt-go v0.1.3
	github.com/jhump/protoreflect v1.10.1
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/lib/pq v1.10.4
	github.com/linkedin/goavro/v2 v2.12.0
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-runewidth v0.0.13 // indirect
	github.com/microcosm-cc/bluemonday v1.0.19
	github.com/mitchellh/mapstructure v1.4.3
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/nats-io/nats-server/v2 v2.8.4 // indirect
	github.com/nats-io/nats-streaming-server v0.24.6 // indirect
	github.com/nats-io/nats.go v1.15.0
	github.com/nats-io/stan.go v0.10.2
	github.com/nsf/jsondiff v0.0.0-20210926074059-1e845ec5d249
	github.com/nsqio/go-nsq v1.1.0
	github.com/olivere/elastic/v7 v7.0.31
	github.com/opencontainers/runc v1.0.3 // indirect
	github.com/opensearch-project/opensearch-go/v2 v2.1.0
	github.com/ory/dockertest/v3 v3.8.1
	github.com/oschwald/geoip2-golang v1.5.0
	github.com/pebbe/zmq4 v1.2.7
	github.com/pierrec/lz4/v4 v4.1.15
	github.com/pkg/sftp v1.13.4
	github.com/prometheus/client_golang v1.11.1
	github.com/prometheus/common v0.32.1
	github.com/pusher/pusher-http-go v4.0.1+incompatible
	github.com/quipo/dependencysolver v0.0.0-20170801134659-2b009cb4ddcc
	github.com/rabbitmq/amqp091-go v1.4.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/rickb777/date v1.17.0
	github.com/rivo/uniseg v0.3.4 // indirect
	github.com/robfig/cron/v3 v3.0.1
	github.com/segmentio/ksuid v1.0.4
	github.com/segmentio/parquet-go v0.0.0-20220830163417-b03c0471ebb0
	github.com/sirupsen/logrus v1.8.1
	github.com/smira/go-statsd v1.3.2
	github.com/snowflakedb/gosnowflake v1.6.6
	github.com/stretchr/testify v1.8.0
	github.com/tidwall/gjson v1.14.2
	github.com/tidwall/sjson v1.2.5
	github.com/tilinna/z85 v1.0.0
	github.com/twmb/franz-go v1.3.1
	github.com/twmb/franz-go/pkg/kmsg v0.0.0-20220106200407-cfd3330d96f5
	github.com/urfave/cli/v2 v2.3.0
	github.com/vmihailenco/msgpack/v5 v5.3.5
	github.com/xdg/scram v1.0.3
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20211228015320-b4f792c43cd0
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a
	go.mongodb.org/mongo-driver v1.8.2
	go.nanomsg.org/mangos/v3 v3.3.0
	go.opentelemetry.io/otel v1.9.0
	go.opentelemetry.io/otel/exporters/jaeger v1.4.1
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.8.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.8.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.8.0
	go.opentelemetry.io/otel/sdk v1.8.0
	go.opentelemetry.io/otel/trace v1.9.0
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0
	golang.org/x/crypto v0.0.0-20220511200225-c6db032c6c88
	golang.org/x/net v0.0.0-20220722155237-a158d28d115b
	golang.org/x/oauth2 v0.0.0-20220309155454-6242fa91716a
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/text v0.3.7
	golang.org/x/time v0.0.0-20220210224613-90d013bbcef8 // indirect
	google.golang.org/api v0.74.0
	google.golang.org/grpc v1.46.2
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v3 v3.0.1
)

go 1.16
