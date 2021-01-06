module github.com/Jeffail/benthos/v3

require (
	cloud.google.com/go v0.71.0 // indirect
	cloud.google.com/go/pubsub v1.8.2
	github.com/Azure/azure-sdk-for-go v48.0.0+incompatible
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.13.1
	github.com/Azure/go-autorest/autorest v0.11.10 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/ClickHouse/clickhouse-go v1.4.3
	github.com/HdrHistogram/hdrhistogram-go v1.0.0 // indirect
	github.com/Jeffail/gabs/v2 v2.6.0
	github.com/Jeffail/grok v1.1.0
	github.com/OneOfOne/xxhash v1.2.8
	github.com/Shopify/sarama v1.27.2
	github.com/armon/go-metrics v0.3.4 // indirect
	github.com/armon/go-radix v1.0.0
	github.com/aws/aws-lambda-go v1.20.0
	github.com/aws/aws-sdk-go v1.35.20
	github.com/benhoyt/goawk v1.6.1
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cenkalti/backoff/v3 v3.2.2 // indirect
	github.com/cenkalti/backoff/v4 v4.1.0
	github.com/clbanning/mxj v1.8.4
	github.com/colinmarc/hdfs v1.1.3
	github.com/containerd/continuity v0.0.0-20200928162600-f2cc35102c2a // indirect
	github.com/dgraph-io/ristretto v0.0.3
	github.com/dnaeon/go-vcr v1.1.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/fatih/color v1.10.0
	github.com/go-redis/redis/v7 v7.4.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gocql/gocql v0.0.0-20201024154641-5913df4d474e
	github.com/gofrs/uuid v3.3.0+incompatible
	github.com/golang/protobuf v1.4.3
	github.com/golang/snappy v0.0.2
	github.com/google/go-cmp v0.5.2
	github.com/google/gofuzz v1.2.0
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.4.2
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/influxdata/go-syslog/v3 v3.0.0
	github.com/influxdata/influxdb1-client v0.0.0-20200827194710-b269163b24ab
	github.com/itchyny/gojq v0.11.2
	github.com/jhump/protoreflect v1.7.0
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.11.2 // indirect
	github.com/lib/pq v1.8.0
	github.com/linkedin/goavro/v2 v2.9.8
	github.com/microcosm-cc/bluemonday v1.0.4
	github.com/moby/term v0.0.0-20201101162038-25d840ce174a // indirect
	github.com/nats-io/jwt v1.2.0 // indirect
	github.com/nats-io/nats-streaming-server v0.19.0 // indirect
	github.com/nats-io/nats.go v1.10.0
	github.com/nats-io/stan.go v0.7.0
	github.com/nsqio/go-nsq v1.0.8
	github.com/olivere/elastic/v7 v7.0.21
	github.com/opentracing/opentracing-go v1.2.0
	github.com/ory/dockertest/v3 v3.6.2
	github.com/patrobinson/gokini v0.1.0
	github.com/pebbe/zmq4 v1.2.1
	github.com/pierrec/lz4 v2.6.0+incompatible // indirect
	github.com/prometheus/client_golang v1.8.0
	github.com/quipo/dependencysolver v0.0.0-20170801134659-2b009cb4ddcc
	github.com/quipo/statsd v0.0.0-20180118161217-3d6a5565f314
	github.com/rcrowley/go-metrics v0.0.0-20200313005456-10cdbea86bc0
	github.com/robfig/cron/v3 v3.0.1
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/sirupsen/logrus v1.7.0 // indirect
	github.com/smira/go-statsd v1.3.1
	github.com/spf13/cast v1.3.1
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.6.1
	github.com/tilinna/z85 v1.0.0
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/urfave/cli/v2 v2.3.0
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	go.nanomsg.org/mangos/v3 v3.1.3
	go.uber.org/atomic v1.7.0 // indirect
	golang.org/x/net v0.0.0-20201031054903-ff519b6c9102
	golang.org/x/oauth2 v0.0.0-20200902213428-5d25da1a8d43
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/text v0.3.4 // indirect
	golang.org/x/tools v0.0.0-20201103190053-ac612affd56b // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20201103154000-415bd0cd5df6 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
)

go 1.13
