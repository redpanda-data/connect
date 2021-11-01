module github.com/Jeffail/benthos/v3

require (
	cloud.google.com/go v0.94.1 // indirect
	cloud.google.com/go/bigquery v1.8.0
	cloud.google.com/go/kms v0.1.0 // indirect
	cloud.google.com/go/pubsub v1.9.1
	cloud.google.com/go/storage v1.16.1
	github.com/Azure/azure-sdk-for-go v48.0.0+incompatible
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.13.1
	github.com/Azure/go-autorest/autorest v0.11.10
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/Jeffail/gabs/v2 v2.6.1
	github.com/Jeffail/grok v1.1.0
	github.com/OneOfOne/xxhash v1.2.8
	github.com/Shopify/sarama v1.28.0
	github.com/apache/pulsar-client-go v0.6.0
	github.com/armon/go-metrics v0.3.4 // indirect
	github.com/armon/go-radix v1.0.0
	github.com/aws/aws-lambda-go v1.20.0
	github.com/aws/aws-sdk-go v1.38.65
	github.com/benhoyt/goawk v1.6.1
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/clbanning/mxj/v2 v2.5.3
	github.com/colinmarc/hdfs v1.1.3
	github.com/containerd/continuity v0.0.0-20200928162600-f2cc35102c2a // indirect
	github.com/denisenkom/go-mssqldb v0.10.0
	github.com/dgraph-io/ristretto v0.0.3
	github.com/dnaeon/go-vcr v1.1.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.3.5
	github.com/edsrzf/mmap-go v1.0.0
	github.com/fatih/color v1.10.0
	github.com/go-redis/redis/v7 v7.4.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20210817081954-bc256bbb90de
	github.com/gofrs/uuid v3.3.0+incompatible
	github.com/golang-jwt/jwt v3.2.1+incompatible
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.6
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/influxdata/go-syslog/v3 v3.0.0
	github.com/influxdata/influxdb1-client v0.0.0-20200827194710-b269163b24ab
	github.com/itchyny/gojq v0.11.2
	github.com/itchyny/timefmt-go v0.1.3
	github.com/jhump/protoreflect v1.7.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.11.12 // indirect
	github.com/lib/pq v1.8.0
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/microcosm-cc/bluemonday v1.0.4
	github.com/mitchellh/mapstructure v1.4.1
	github.com/moby/term v0.0.0-20201101162038-25d840ce174a // indirect
	github.com/nats-io/jwt v1.2.0 // indirect
	github.com/nats-io/nats-streaming-server v0.19.0 // indirect
	github.com/nats-io/nats.go v1.12.3
	github.com/nats-io/stan.go v0.7.0
	github.com/nsf/jsondiff v0.0.0-20200515183724-f29ed568f4ce
	github.com/nsqio/go-nsq v1.0.8
	github.com/olivere/elastic/v7 v7.0.21
	github.com/opentracing/opentracing-go v1.2.0
	github.com/ory/dockertest/v3 v3.6.3
	github.com/patrobinson/gokini v0.1.0
	github.com/pebbe/zmq4 v1.2.1
	github.com/pierrec/lz4/v4 v4.1.7
	github.com/pkg/sftp v1.12.0
	github.com/prometheus/client_golang v1.8.0
	github.com/quipo/dependencysolver v0.0.0-20170801134659-2b009cb4ddcc
	github.com/quipo/statsd v0.0.0-20180118161217-3d6a5565f314
	github.com/rabbitmq/amqp091-go v1.1.1-0.20211014165712-60a96d570ece
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/robfig/cron/v3 v3.0.1
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.7.0 // indirect
	github.com/smira/go-statsd v1.3.1
	github.com/spf13/cast v1.3.1
	github.com/stretchr/testify v1.7.0
	github.com/tilinna/z85 v1.0.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/urfave/cli/v2 v2.3.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	go.mongodb.org/mongo-driver v1.4.4
	go.nanomsg.org/mangos/v3 v3.1.3
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/net v0.0.0-20210902165921-8d991716f632
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/sys v0.0.0-20210902050250-f475640dd07b // indirect
	golang.org/x/text v0.3.7
	google.golang.org/api v0.56.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

// This stinks, and is due to https://github.com/99designs/keyring/issues/64
replace github.com/keybase/go-keychain => github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4

go 1.16
