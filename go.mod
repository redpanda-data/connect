module github.com/redpanda-data/connect/v4

go 1.25.6

replace github.com/99designs/keyring => github.com/Jeffail/keyring v1.2.3

ignore (
	./bin
	./config
	./dist
	./docs
	./licenses
	./proto
	./resources
	./target
	./taskfiles
)

require (
	buf.build/gen/go/bufbuild/reflect/connectrpc/go v1.19.1-20240117202343-bf8f65e8876c.2
	buf.build/gen/go/bufbuild/reflect/protocolbuffers/go v1.36.11-20240117202343-bf8f65e8876c.1
	buf.build/go/hyperpb v0.1.3
	cloud.google.com/go/aiplatform v1.113.0
	cloud.google.com/go/bigquery v1.72.0
	cloud.google.com/go/pubsub v1.50.1
	cloud.google.com/go/spanner v1.87.0
	cloud.google.com/go/storage v1.59.1
	cloud.google.com/go/vertexai v0.15.0
	connectrpc.com/connect v1.19.1
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.21.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1
	github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos v1.4.2
	github.com/Azure/azure-sdk-for-go/sdk/data/aztables v1.4.1
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.4
	github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake v1.4.4
	github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue v1.0.1
	github.com/Azure/go-amqp v1.5.1
	github.com/ClickHouse/clickhouse-go/v2 v2.42.0
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace v1.30.0
	github.com/IBM/sarama v1.46.3
	github.com/Jeffail/checkpoint v1.1.0
	github.com/Jeffail/gabs/v2 v2.7.0
	github.com/Jeffail/shutdown v1.1.0
	github.com/Masterminds/semver v1.5.0
	github.com/Masterminds/squirrel v1.5.4
	github.com/PaesslerAG/gval v1.2.4
	github.com/PaesslerAG/jsonpath v0.1.1
	github.com/a2aproject/a2a-go v0.3.4
	github.com/apache/pulsar-client-go v0.18.0
	github.com/auth0/go-jwt-middleware/v2 v2.3.1
	github.com/authzed/authzed-go v1.7.0
	github.com/authzed/grpcutil v0.0.0-20250221190651-1985b19b35b8
	github.com/aws/aws-lambda-go v1.52.0
	github.com/aws/aws-sdk-go-v2 v1.41.1
	github.com/aws/aws-sdk-go-v2/config v1.32.7
	github.com/aws/aws-sdk-go-v2/credentials v1.19.7
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression v1.8.30
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.20.19
	github.com/aws/aws-sdk-go-v2/service/bedrockruntime v1.47.2
	github.com/aws/aws-sdk-go-v2/service/cloudwatch v1.53.1
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.53.6
	github.com/aws/aws-sdk-go-v2/service/firehose v1.42.9
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.42.10
	github.com/aws/aws-sdk-go-v2/service/lambda v1.87.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.95.1
	github.com/aws/aws-sdk-go-v2/service/sns v1.39.11
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.21
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.6
	github.com/beanstalkd/go-beanstalk v0.2.0
	github.com/benhoyt/goawk v1.31.0
	github.com/bmatcuk/doublestar/v4 v4.9.2
	github.com/bradfitz/gomemcache v0.0.0-20250403215159-8d39553ac7cf
	github.com/bufbuild/prototransform v0.4.0
	github.com/bwmarrin/discordgo v0.29.0
	github.com/bwmarrin/snowflake v0.3.0
	github.com/cenkalti/backoff/v4 v4.3.0
	github.com/clbanning/mxj/v2 v2.7.0
	github.com/colinmarc/hdfs v1.1.3
	github.com/couchbase/gocb/v2 v2.11.1
	github.com/cyborginc/cyborgdb-go v0.14.0
	github.com/databricks/databricks-sql-go v1.9.0
	github.com/dgraph-io/ristretto/v2 v2.3.0
	github.com/dop251/goja v0.0.0-20250630131328-58d95d85e994
	github.com/dop251/goja_nodejs v0.0.0-20250409162600-f7acab6894b0
	github.com/dustin/go-humanize v1.0.1
	github.com/ebitengine/purego v0.9.1
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/elastic/elastic-transport-go/v8 v8.8.0
	github.com/elastic/go-elasticsearch/v8 v8.19.1
	github.com/elastic/go-elasticsearch/v9 v9.2.1
	github.com/generikvault/gvalstrings v0.0.0-20180926130504-471f38f0112a
	github.com/getsentry/sentry-go v0.41.0
	github.com/go-faker/faker/v4 v4.7.0
	github.com/go-git/go-git/v5 v5.16.4
	github.com/go-jose/go-jose/v4 v4.1.3
	github.com/go-mysql-org/go-mysql v1.13.0
	github.com/go-resty/resty/v2 v2.17.1
	github.com/go-sql-driver/mysql v1.9.3
	github.com/go-viper/mapstructure/v2 v2.5.0
	github.com/gocql/gocql v1.7.0
	github.com/gofrs/uuid/v5 v5.4.0
	github.com/golang-jwt/jwt/v5 v5.3.0
	github.com/google/go-cmp v0.7.0
	github.com/googleapis/go-sql-spanner v1.22.0
	github.com/gosimple/slug v1.15.0
	github.com/hamba/avro/v2 v2.31.0
	github.com/influxdata/influxdb1-client v0.0.0-20220302092344-a9ab5670611c
	github.com/jackc/pgx/v4 v4.18.3
	github.com/jackc/pgx/v5 v5.8.0
	github.com/jhump/protoreflect v1.17.0
	github.com/lib/pq v1.10.9
	github.com/linkedin/goavro/v2 v2.14.1
	github.com/matoous/go-nanoid/v2 v2.1.0
	github.com/microcosm-cc/bluemonday v1.0.27
	github.com/microsoft/go-mssqldb v1.9.5
	github.com/microsoft/gocosmos v1.1.1
	github.com/modelcontextprotocol/go-sdk v1.2.0
	github.com/nats-io/nats.go v1.48.0
	github.com/nats-io/nkeys v0.4.12
	github.com/nats-io/stan.go v0.10.4
	github.com/neo4j/neo4j-go-driver/v5 v5.28.4
	github.com/nsf/jsondiff v0.0.0-20230430225905-43f6cf3098c1
	github.com/nsqio/go-nsq v1.1.0
	github.com/oauth2-proxy/mockoidc v0.0.0-20240214162133-caebfff84d25
	github.com/oklog/ulid/v2 v2.1.1
	github.com/opensearch-project/opensearch-go/v3 v3.1.0
	github.com/ory/dockertest/v3 v3.12.0
	github.com/oschwald/geoip2-golang v1.13.0
	github.com/parquet-go/parquet-go v0.27.0
	github.com/pebbe/zmq4 v1.4.0
	github.com/pinecone-io/go-pinecone v1.1.1
	github.com/pkg/sftp v1.13.10
	github.com/pkoukk/tiktoken-go v0.1.8
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/common v0.67.5
	github.com/pusher/pusher-http-go v4.0.1+incompatible
	github.com/qdrant/go-client v1.16.2
	github.com/questdb/go-questdb-client/v4 v4.1.0
	github.com/r3labs/diff/v3 v3.0.2
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9
	github.com/redis/go-redis/v9 v9.17.2
	github.com/redpanda-data/benthos/v4 v4.63.1
	github.com/redpanda-data/common-go/authz v0.2.0
	github.com/redpanda-data/common-go/license v0.0.0-20260109170727-1dd9f5d22ee1
	github.com/redpanda-data/common-go/redpanda-otel-exporter v0.3.0
	github.com/redpanda-data/common-go/secrets v0.1.15
	github.com/redpanda-data/connect/public/bundle/free/v4 v4.78.0
	github.com/rs/xid v1.6.0
	github.com/sashabaranov/go-openai v1.41.2
	github.com/sijms/go-ora/v2 v2.9.0
	github.com/slack-go/slack v0.17.3
	github.com/smira/go-statsd v1.3.4
	github.com/snowflakedb/gosnowflake v1.18.1
	github.com/sourcegraph/conc v0.3.0
	github.com/stretchr/testify v1.11.1
	github.com/testcontainers/testcontainers-go/modules/ollama v0.40.0
	github.com/testcontainers/testcontainers-go/modules/qdrant v0.40.0
	github.com/tetratelabs/wazero v1.11.0
	github.com/tigerbeetle/tigerbeetle-go v0.16.68
	github.com/timeplus-io/proton-go-driver/v2 v2.1.3
	github.com/tmc/langchaingo v0.1.14
	github.com/trinodb/trino-go-client v0.333.0
	github.com/twmb/franz-go v1.20.6
	github.com/twmb/franz-go/pkg/kadm v1.17.1
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20251115002817-3affad808a82
	github.com/twmb/franz-go/pkg/kmsg v1.12.0
	github.com/twmb/franz-go/pkg/sr v1.6.0
	github.com/twmb/go-cache v1.2.1
	github.com/vmihailenco/msgpack/v5 v5.4.1
	github.com/xdg-go/scram v1.2.0
	github.com/xeipuuv/gojsonschema v1.2.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20241021075129-b732d2ac9c9b
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78
	go.mongodb.org/mongo-driver/v2 v2.4.1
	go.nanomsg.org/mangos/v3 v3.4.2
	go.opentelemetry.io/collector/pdata v1.49.0
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.15.0
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.15.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.39.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.39.0
	go.opentelemetry.io/otel/log v0.15.0
	go.opentelemetry.io/otel/sdk v1.39.0
	go.opentelemetry.io/otel/sdk/log v0.15.0
	go.opentelemetry.io/otel/sdk/metric v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
	go.starlark.net v0.0.0-20250318223901-d9371fef63fe
	go.uber.org/multierr v1.11.0
	golang.org/x/crypto v0.47.0
	golang.org/x/net v0.49.0
	golang.org/x/sync v0.19.0
	golang.org/x/text v0.33.0
	google.golang.org/api v0.257.0
	google.golang.org/protobuf v1.36.11
	modernc.org/sqlite v1.44.1
)

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.10-20250912141014-52f32327d4b0.1 // indirect
	cel.dev/expr v0.24.0 // indirect
	cloud.google.com/go/longrunning v0.7.0 // indirect
	cloud.google.com/go/monitoring v1.24.3 // indirect
	cloud.google.com/go/pubsub/v2 v2.1.0 // indirect
	cloud.google.com/go/secretmanager v1.15.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/azsecrets v0.12.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/keyvault/internal v0.7.1 // indirect
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/GoogleCloudPlatform/grpc-gcp-go/grpcgcp v1.5.3 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.30.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.54.0 // indirect
	github.com/ProtonMail/go-crypto v1.3.0 // indirect
	github.com/RoaringBitmap/roaring/v2 v2.8.0 // indirect
	github.com/apache/arrow-go/v18 v18.4.1 // indirect
	github.com/apache/arrow/go/v12 v12.0.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.39.6 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.5 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/certifi/gocertifi v0.0.0-20210507211836-431795d63e8d // indirect
	github.com/cloudflare/circl v1.6.1 // indirect
	github.com/cncf/xds/go v0.0.0-20251022180443-0feb69152e9f // indirect
	github.com/containerd/errdefs v1.0.0 // indirect
	github.com/containerd/errdefs/pkg v0.3.0 // indirect
	github.com/containerd/platforms v1.0.0-rc.1 // indirect
	github.com/coreos/go-oidc/v3 v3.5.0 // indirect
	github.com/cyphar/filepath-securejoin v0.5.0 // indirect
	github.com/dnephin/pflag v1.0.7 // indirect
	github.com/emirpasic/gods v1.18.1 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.35.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.2.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/go-git/gcfg v1.5.1-0.20230307220236-3a3c6141e376 // indirect
	github.com/go-git/go-billy/v5 v5.6.2 // indirect
	github.com/go-jose/go-jose/v3 v3.0.4 // indirect
	github.com/go-quicktest/qt v1.101.1-0.20240301121107-c6c8733fa1e6 // indirect
	github.com/google/jsonschema-go v0.3.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.7 // indirect
	github.com/hashicorp/go-version v1.8.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jzelinskie/stringz v0.0.3 // indirect
	github.com/kevinburke/ssh_config v1.4.0 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/knadh/koanf/maps v0.1.2 // indirect
	github.com/knadh/koanf/parsers/yaml v1.1.0 // indirect
	github.com/knadh/koanf/providers/file v1.2.0 // indirect
	github.com/knadh/koanf/providers/rawbytes v1.0.0 // indirect
	github.com/knadh/koanf/v2 v2.3.0 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/go-archive v0.1.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/onsi/gomega v1.38.2 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pingcap/errors v0.11.5-0.20250318082626-8f80e5cb09ec // indirect
	github.com/pingcap/failpoint v0.0.0-20240528011301-b51a646c7c86 // indirect
	github.com/pingcap/log v1.1.1-0.20241212030209-7e3ff8601a2a // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20250421232622-526b2c79173d // indirect
	github.com/pjbgf/sha1cd v0.5.0 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/rs/zerolog v1.31.0 // indirect
	github.com/samber/lo v1.52.0 // indirect
	github.com/sergi/go-diff v1.4.0 // indirect
	github.com/shirou/gopsutil/v4 v4.25.10 // indirect
	github.com/skeema/knownhosts v1.3.1 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/theparanoids/crypki v1.20.9 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/timandy/routine v1.1.5 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/xanzy/ssh-agent v0.3.3 // indirect
	github.com/yosida95/uritemplate/v3 v3.0.2 // indirect
	gitlab.com/golang-commonmark/html v0.0.0-20191124015941-a22733972181 // indirect
	gitlab.com/golang-commonmark/linkify v0.0.0-20200225224916-64bca66f6ad3 // indirect
	gitlab.com/golang-commonmark/mdurl v0.0.0-20191124015652-932350d1cb84 // indirect
	gitlab.com/golang-commonmark/puny v0.0.0-20191124015043-9f83538fa04f // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/collector/featuregate v1.49.0 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.38.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20251023183803-a4bb9ffd2546 // indirect
	golang.org/x/telemetry v0.0.0-20251203150158-8fff8a5912fc // indirect
	gopkg.in/go-jose/go-jose.v2 v2.6.3 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	gotest.tools/gotestsum v1.8.2 // indirect
	k8s.io/apimachinery v0.34.1 // indirect
	k8s.io/client-go v0.34.1 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/utils v0.0.0-20251002143259-bc988d571ff4 // indirect
	sigs.k8s.io/json v0.0.0-20250730193827-2d320260d730 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
	sigs.k8s.io/structured-merge-diff/v6 v6.3.0 // indirect
)

require (
	cloud.google.com/go v0.123.0 // indirect
	cloud.google.com/go/auth v0.18.0
	cloud.google.com/go/auth/oauth2adapt v0.2.8 // indirect
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/iam v1.5.3 // indirect
	cloud.google.com/go/trace v1.11.6 // indirect
	cuelang.org/go v0.15.1 // indirect
	dario.cat/mergo v1.0.2 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/99designs/go-keychain v0.0.0-20191008050251-8e49817e8af4 // indirect
	github.com/99designs/keyring v1.2.2 // indirect
	github.com/AthenZ/athenz v1.12.26 // indirect
	github.com/Azure/azure-sdk-for-go v68.0.0+incompatible // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20250102033503-faa5f7b0171c // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/ClickHouse/ch-go v0.69.0 // indirect
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.54.0 // indirect
	github.com/Jeffail/grok v1.1.0 // indirect
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/OneOfOne/xxhash v1.2.8 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/apache/arrow/go/arrow v0.0.0-20211112161151-bc219186db40 // indirect
	github.com/apache/arrow/go/v15 v15.0.2 // indirect
	github.com/apache/thrift v0.22.0 // indirect
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/ardielle/ardielle-go v1.5.2 // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.30 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.17 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.17
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.32.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.13 // indirect
	github.com/aws/smithy-go v1.24.0
	github.com/aymerick/douceur v0.2.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.24.0 // indirect
	github.com/btnguyen2k/consu/checksum v1.1.1 // indirect
	github.com/btnguyen2k/consu/g18 v0.1.0 // indirect
	github.com/btnguyen2k/consu/gjrc v0.2.2 // indirect
	github.com/btnguyen2k/consu/olaf v0.1.3 // indirect
	github.com/btnguyen2k/consu/reddo v0.1.9 // indirect
	github.com/btnguyen2k/consu/semita v0.1.5 // indirect
	github.com/bufbuild/protocompile v0.14.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cockroachdb/apd/v3 v3.2.1 // indirect
	github.com/cohere-ai/cohere-go/v2 v2.16.1
	github.com/containerd/continuity v0.4.5 // indirect
	github.com/containerd/log v0.1.0 // indirect
	github.com/couchbase/gocbcore/v10 v10.8.1 // indirect
	github.com/couchbase/gocbcoreps v0.1.4 // indirect
	github.com/couchbase/goprotostellar v1.0.2 // indirect
	github.com/couchbaselabs/gocbconnstr/v2 v2.0.0 // indirect
	github.com/cpuguy83/dockercfg v0.3.2 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/danieljoos/wincred v1.2.3 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dlclark/regexp2 v1.11.5 // indirect
	github.com/docker/cli v28.4.0+incompatible // indirect
	github.com/docker/docker v28.5.2+incompatible // indirect
	github.com/docker/go-connections v0.6.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dvsekhvalnov/jose2go v1.8.0 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/color v1.18.0
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.10 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-sourcemap/sourcemap v2.1.4+incompatible // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/groupcache v0.0.0-20241129210726-2c02b8208cf8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.9.23+incompatible // indirect
	github.com/google/pprof v0.0.0-20251002213607-436353cc1ee6 // indirect
	github.com/google/s2a-go v0.1.9 // indirect
	github.com/google/shlex v0.0.0-20191202100458-e7afc7fbc510 // indirect
	github.com/google/uuid v1.6.0
	github.com/googleapis/enterprise-certificate-proxy v0.3.7 // indirect
	github.com/googleapis/gax-go/v2 v2.15.0 // indirect
	github.com/gorilla/css v1.0.1 // indirect
	github.com/gorilla/handlers v1.5.2
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674 // indirect
	github.com/gosimple/unidecode v1.0.1 // indirect
	github.com/govalues/decimal v0.1.36 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.3 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/influxdata/go-syslog/v3 v3.0.0 // indirect
	github.com/itchyny/gojq v0.12.18 // indirect
	github.com/itchyny/timefmt-go v0.1.7 // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.14.3
	github.com/jackc/pgio v1.0.0
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgtype v1.14.4
	github.com/jackc/puddle v1.3.0 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.18.3
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/pgzip v1.2.6 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lufia/plan9stats v0.0.0-20251013123823-9fd1530e3ec3 // indirect
	github.com/magiconair/properties v1.8.10 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/moby/docker-image-spec v1.3.1 // indirect
	github.com/moby/patternmatcher v0.6.0 // indirect
	github.com/moby/sys/sequential v0.6.0 // indirect
	github.com/moby/sys/user v0.4.0 // indirect
	github.com/moby/term v0.5.2 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/mtibben/percent v0.2.1 // indirect
	github.com/nats-io/nats-server/v2 v2.9.23 // indirect
	github.com/nats-io/nats-streaming-server v0.24.6 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/oapi-codegen/runtime v1.1.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/opencontainers/runc v1.3.1 // indirect
	github.com/oschwald/maxminddb-golang v1.13.1 // indirect
	github.com/paulmach/orb v0.12.0 // indirect
	github.com/pgvector/pgvector-go v0.3.0
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/quipo/dependencysolver v0.0.0-20170801134659-2b009cb4ddcc // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rickb777/period v1.0.21 // indirect
	github.com/rickb777/plural v1.4.7 // indirect
	github.com/rivo/uniseg v0.4.7
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/segmentio/encoding v0.5.3
	github.com/segmentio/ksuid v1.0.4 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	github.com/testcontainers/testcontainers-go v0.40.0 // indirect
	github.com/testcontainers/testcontainers-go/modules/mongodb v0.40.0
	github.com/tilinna/z85 v1.0.0 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/urfave/cli/v2 v2.27.7
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xrash/smetrics v0.0.0-20250705151800-55b8f293f342 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	gitlab.com/golang-commonmark/markdown v0.0.0-20211110145824-bf3e522c626a // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.63.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.63.0 // indirect
	go.opentelemetry.io/otel/metric v1.39.0
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/mod v0.31.0 // indirect
	golang.org/x/oauth2 v0.34.0
	golang.org/x/sys v0.40.0
	golang.org/x/term v0.39.0 // indirect
	golang.org/x/time v0.14.0 // indirect
	golang.org/x/tools v0.40.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/genai v1.42.0
	google.golang.org/genproto v0.0.0-20251002232023-7c0ddcbb5797 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20251202230838-ff82c1b0f217 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251213004720-97cd9d5aeac2 // indirect
	google.golang.org/grpc v1.78.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/libc v1.67.6 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)
