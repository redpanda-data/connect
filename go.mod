module github.com/benthosdev/benthos/v4

require (
	cloud.google.com/go/bigquery v1.44.0
	cloud.google.com/go/pubsub v1.27.1
	cloud.google.com/go/storage v1.28.0
	cuelang.org/go v0.4.2
	github.com/Azure/azure-sdk-for-go v61.1.0+incompatible
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.1.4
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v1.0.1
	github.com/Azure/azure-storage-queue-go v0.0.0-20191125232315-636801874cdd
	github.com/Azure/go-amqp v0.17.0
	github.com/Azure/go-autorest/autorest v0.11.23
	github.com/ClickHouse/clickhouse-go/v2 v2.6.1
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.4.0
	github.com/Jeffail/gabs/v2 v2.7.0
	github.com/Jeffail/grok v1.1.0
	github.com/Masterminds/squirrel v1.5.2
	github.com/OneOfOne/xxhash v1.2.8
	github.com/PaesslerAG/gval v1.2.2
	github.com/PaesslerAG/jsonpath v0.1.1
	github.com/Shopify/sarama v1.30.1
	github.com/apache/pulsar-client-go v0.8.1
	github.com/aws/aws-lambda-go v1.28.0
	github.com/aws/aws-sdk-go v1.42.31
	github.com/beanstalkd/go-beanstalk v0.2.0
	github.com/benhoyt/goawk v1.21.0
	github.com/bradfitz/gomemcache v0.0.0-20230124162541-5f7a7d875746
	github.com/bwmarrin/snowflake v0.3.0
	github.com/bxcodec/faker/v3 v3.8.1
	github.com/cenkalti/backoff/v4 v4.2.0
	github.com/clbanning/mxj/v2 v2.5.7
	github.com/colinmarc/hdfs v1.1.3
	github.com/couchbase/gocb/v2 v2.6.0
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/dgraph-io/ristretto v0.1.1
	github.com/dustin/go-humanize v1.0.1
	github.com/eclipse/paho.mqtt.golang v1.4.2
	github.com/fatih/color v1.14.1
	github.com/fsnotify/fsnotify v1.6.0
	github.com/generikvault/gvalstrings v0.0.0-20180926130504-471f38f0112a
	github.com/go-sql-driver/mysql v1.6.0
	github.com/gocql/gocql v1.3.1
	github.com/gofrs/uuid v4.4.0+incompatible
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/golang-jwt/jwt/v4 v4.4.3
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.9
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.5.0
	github.com/gosimple/slug v1.13.1
	github.com/influxdata/go-syslog/v3 v3.0.0
	github.com/influxdata/influxdb1-client v0.0.0-20220302092344-a9ab5670611c
	github.com/itchyny/gojq v0.12.11
	github.com/itchyny/timefmt-go v0.1.5
	github.com/jhump/protoreflect v1.14.1
	github.com/jmespath/go-jmespath v0.4.0
	github.com/klauspost/compress v1.15.15
	github.com/lib/pq v1.10.4
	github.com/linkedin/goavro/v2 v2.12.0
	github.com/matoous/go-nanoid/v2 v2.0.0
	github.com/microcosm-cc/bluemonday v1.0.22
	github.com/mitchellh/mapstructure v1.4.3
	github.com/nats-io/nats.go v1.23.0
	github.com/nats-io/nkeys v0.3.0
	github.com/nats-io/stan.go v0.10.2
	github.com/nsf/jsondiff v0.0.0-20210926074059-1e845ec5d249
	github.com/nsqio/go-nsq v1.1.0
	github.com/olivere/elastic/v7 v7.0.31
	github.com/ory/dockertest/v3 v3.8.1
	github.com/oschwald/geoip2-golang v1.5.0
	github.com/pebbe/zmq4 v1.2.7
	github.com/pierrec/lz4/v4 v4.1.17
	github.com/pkg/sftp v1.13.5
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/common v0.39.0
	github.com/pusher/pusher-http-go v4.0.1+incompatible
	github.com/quipo/dependencysolver v0.0.0-20170801134659-2b009cb4ddcc
	github.com/rabbitmq/amqp091-go v1.4.0
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/redis/go-redis/v9 v9.0.2
	github.com/rickb777/date v1.17.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/segmentio/ksuid v1.0.4
	github.com/segmentio/parquet-go v0.0.0-20220830163417-b03c0471ebb0
	github.com/sijms/go-ora/v2 v2.5.22
	github.com/sirupsen/logrus v1.9.0
	github.com/smira/go-statsd v1.3.2
	github.com/snowflakedb/gosnowflake v1.6.16
	github.com/stretchr/testify v1.8.1
	github.com/tetratelabs/wazero v1.0.0-pre.9
	github.com/tilinna/z85 v1.0.0
	github.com/twmb/franz-go v1.12.1
	github.com/twmb/franz-go/pkg/kmsg v1.4.0
	github.com/urfave/cli/v2 v2.11.0
	github.com/vmihailenco/msgpack/v5 v5.3.5
	github.com/xdg/scram v1.0.3
	github.com/xeipuuv/gojsonschema v1.2.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20211228015320-b4f792c43cd0
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a
	go.mongodb.org/mongo-driver v1.8.2
	go.nanomsg.org/mangos/v3 v3.3.0
	go.opentelemetry.io/otel v1.13.0
	go.opentelemetry.io/otel/exporters/jaeger v1.13.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.11.2
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.11.2
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.11.2
	go.opentelemetry.io/otel/sdk v1.13.0
	go.opentelemetry.io/otel/trace v1.13.0
	go.uber.org/multierr v1.9.0
	golang.org/x/crypto v0.6.0
	golang.org/x/net v0.6.0
	golang.org/x/oauth2 v0.5.0
	golang.org/x/sync v0.1.0
	golang.org/x/text v0.7.0
	google.golang.org/api v0.103.0
	google.golang.org/grpc v1.53.0
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.19.1
)

require (
	cloud.google.com/go v0.107.0 // indirect
	cloud.google.com/go/compute v1.15.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.8.0 // indirect
	cloud.google.com/go/trace v1.4.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.1 // indirect
	github.com/AthenZ/athenz v1.10.43 // indirect
	github.com/Azure/azure-pipeline-go v0.2.3 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.0.0 // indirect
	github.com/Azure/azure-storage-blob-go v0.15.0 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.18 // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/ClickHouse/ch-go v0.52.0 // indirect
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/Microsoft/go-winio v0.5.2 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40 // indirect
	github.com/apache/pulsar-client-go/oauth2 v0.0.0-20220524063205-c41616b2f512 // indirect
	github.com/apache/thrift v0.17.0 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/armon/go-metrics v0.3.4 // indirect
	github.com/aws/aws-sdk-go-v2 v1.16.16 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.8 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.12.20 // indirect
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.11.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.23 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.18 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.27.11 // indirect
	github.com/aws/smithy-go v1.13.3 // indirect
	github.com/aymerick/douceur v0.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/apd/v2 v2.0.2 // indirect
	github.com/containerd/continuity v0.3.0 // indirect
	github.com/couchbase/gocbcore/v10 v10.2.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/danieljoos/wincred v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/docker/cli v23.0.1+incompatible // indirect
	github.com/docker/docker v23.0.1+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dvsekhvalnov/jose2go v1.5.0 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230111030713-bf00bc1b83b6 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/form3tech-oss/jwt-go v3.2.5+incompatible // indirect
	github.com/gabriel-vasile/mimetype v1.4.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.6.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/godbus/dbus v0.0.0-20190726142602-4481cbc300e2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v23.1.21+incompatible // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.2 // indirect
	github.com/googleapis/gax-go/v2 v2.7.0 // indirect
	github.com/gorilla/css v1.0.0 // indirect
	github.com/gosimple/unidecode v1.0.1 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.13.0 // indirect
	github.com/gsterjov/go-libsecret v0.0.0-20161001094733-a6f4afe4910c // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-ieproxy v0.0.1 // indirect
	github.com/mattn/go-isatty v0.0.17 // indirect
	github.com/mattn/go-runewidth v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/mpvl/unique v0.0.0-20150818121801-cbe035fff7de // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/nats-io/nats-server/v2 v2.8.4 // indirect
	github.com/nats-io/nats-streaming-server v0.24.6 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/onsi/ginkgo v1.16.5 // indirect
	github.com/onsi/gomega v1.18.1 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.3-0.20211202183452-c5a74bcca799 // indirect
	github.com/opencontainers/runc v1.1.4 // indirect
	github.com/oschwald/maxminddb-golang v1.8.0 // indirect
	github.com/paulmach/orb v0.8.0 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pkg/browser v0.0.0-20210911075715-681adbf594b8 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rickb777/plural v1.4.1 // indirect
	github.com/rivo/uniseg v0.3.4 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/segmentio/encoding v0.3.5 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.1 // indirect
	github.com/xdg-go/stringprep v1.0.3 // indirect
	github.com/xdg/stringprep v1.0.3 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.13.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	golang.org/x/exp v0.0.0-20220827204233-334a2380cb91 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/term v0.5.0 // indirect
	golang.org/x/tools v0.6.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	gonum.org/v1/gonum v0.11.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.40.0 // indirect
	modernc.org/ccgo/v3 v3.16.13 // indirect
	modernc.org/libc v1.22.2 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.5.0 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.1.0 // indirect
)

go 1.18
