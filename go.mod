module github.com/Jeffail/benthos/v3

require (
	cloud.google.com/go v0.51.0 // indirect
	cloud.google.com/go/pubsub v1.1.0
	github.com/Azure/go-ansiterm v0.0.0-20170929234023-d6e3b3328b78 // indirect
	github.com/Jeffail/gabs/v2 v2.3.0
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/Nvveen/Gotty v0.0.0-20120604004816-cd527374f1e5 // indirect
	github.com/OneOfOne/xxhash v1.2.7
	github.com/Shopify/sarama v1.25.0
	github.com/armon/go-radix v1.0.0
	github.com/aws/aws-lambda-go v1.13.3
	github.com/aws/aws-sdk-go v1.28.1
	github.com/benhoyt/goawk v1.6.0
	github.com/boltdb/bolt v1.3.1
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/cenkalti/backoff v2.2.1+incompatible
	github.com/clbanning/mxj v1.8.4
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/colinmarc/hdfs v1.1.3
	github.com/containerd/continuity v0.0.0-20181203112020-004b46473808 // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.3.3 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0
	github.com/edsrzf/mmap-go v1.0.0
	github.com/frankban/quicktest v1.4.2 // indirect
	github.com/go-redis/redis v6.15.6+incompatible
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gofrs/uuid v3.2.0+incompatible
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/google/go-cmp v0.4.0 // indirect
	github.com/google/uuid v1.1.1 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20181103185306-d547d1d9531e // indirect
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.1
	github.com/gotestyourself/gotestyourself v2.2.0+incompatible // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/influxdata/go-syslog/v3 v3.0.0
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jmespath/go-jmespath v0.0.0-20180206201540-c2b33e8439af
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/lib/pq v1.3.0
	github.com/linkedin/goavro/v2 v2.9.7
	github.com/mailru/easyjson v0.7.0 // indirect
	github.com/microcosm-cc/bluemonday v1.0.2
	github.com/nats-io/jwt v0.3.2 // indirect
	github.com/nats-io/nats-streaming-server v0.16.1-0.20190905144423-ed7405a40a25 // indirect
	github.com/nats-io/nats.go v1.9.1
	github.com/nats-io/stan.go v0.6.0
	github.com/nsqio/go-nsq v1.0.8
	github.com/olivere/elastic v6.2.27+incompatible
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/opencontainers/image-spec v1.0.1 // indirect
	github.com/opencontainers/runc v0.1.1 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/ory/dockertest v3.3.4+incompatible
	github.com/patrobinson/gokini v0.0.7
	github.com/pebbe/zmq4 v1.0.0
	github.com/pierrec/lz4 v2.4.0+incompatible // indirect
	github.com/pkg/errors v0.9.0 // indirect
	github.com/prometheus/client_golang v1.3.0
	github.com/prometheus/common v0.8.0 // indirect
	github.com/quipo/dependencysolver v0.0.0-20170801134659-2b009cb4ddcc
	github.com/quipo/statsd v0.0.0-20180118161217-3d6a5565f314
	github.com/rcrowley/go-metrics v0.0.0-20190826022208-cac0b30c2563 // indirect
	github.com/smartystreets/assertions v0.0.0-20190215210624-980c5ac6f3ac // indirect
	github.com/smartystreets/goconvey v0.0.0-20190222223459-a17d461953aa // indirect
	github.com/smira/go-statsd v1.3.1
	github.com/spf13/cast v1.3.1
	github.com/streadway/amqp v0.0.0-20200108173154-1c71cc93ed71
	github.com/stretchr/testify v1.4.0
	github.com/trivago/grok v1.0.0
	github.com/trivago/tgo v1.0.5 // indirect
	github.com/uber/jaeger-client-go v2.21.1+incompatible
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c
	github.com/xeipuuv/gojsonpointer v0.0.0-20190905194746-02993c407bfb // indirect
	github.com/xeipuuv/gojsonschema v1.2.0
	go.uber.org/atomic v1.5.1 // indirect
	golang.org/x/crypto v0.0.0-20200109152110-61a87790db17 // indirect
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	golang.org/x/sys v0.0.0-20200113162924-86b910548bc1 // indirect
	golang.org/x/tools v0.0.0-20200305224536-de023d59a5d1 // indirect
	google.golang.org/genproto v0.0.0-20200113173426-e1de0a7b01eb // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.3.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20191120175047-4206685974f2
	gotest.tools v2.2.0+incompatible // indirect
	nanomsg.org/go-mangos v1.4.0
)

go 1.13
