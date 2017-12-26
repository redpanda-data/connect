Environment Vars Config
=======================

The default environment variables based config is a simple bridge config with
env variables for all important fields. The full list of variables and their
default values:

``` sh
BENTHOS_INPUT          =
BENTHOS_OUTPUT         =
BENTHOS_LOG_LEVEL      = INFO

ZMQ_URLS               =
ZMQ_BIND               = true
ZMQ_SOCKET             = PULL

NANOMSG_URLS           =
NANOMSG_BIND           = true
NANOMSG_SOCKET         = PULL

KAFKA_BROKER_ADDRESSES =
KAFKA_CLIENT_ID        = benthos-client
KAFKA_CONSUMER_GROUP   = benthos-group
KAFKA_TOPIC            = benthos-stream
KAFKA_PARTITION        = 0
KAFKA_START_OLDEST     = true
KAFKA_ACK_REP          = true             # Used for output only

AMQP_URL               =
AMQP_EXCHANGE          = benthos-exchange
AMQP_EXCHANGE_TYPE     = direct
AMQP_QUEUE             = benthos-stream
AMQP_KEY               = benthos-key
AMQP_CONSUMER_TAG      = benthos-consumer

NSQD_TCP_ADDRESSES     =                  # Used for input only
NSQD_LOOKUP_ADDRESSES  =                  # Used for input only
NSQ_TCP_ADDRESS        =                  # Used for output only
NSQ_TOPIC              = benthos-messages
NSQ_CHANNEL            = benthos-stream
NSQ_USER_AGENT         = benthos-consumer

NATS_URLS              =
NATS_SUBJECT           = benthos-stream
NATS_CLUSTER_ID        = benthos-cluster
NATS_CLIENT_ID         = benthos-consumer
NATS_QUEUE             = benthos-queue
NATS_DURABLE_NAME      = benthos-offset

REDIS_URL              =
REDIS_CHANNEL          = benthos-stream

HTTP_SERVER_ADDRESS    =
HTTP_SERVER_PATH       = /post
HTTP_CLIENT_URL        =

FILE_PATH              =
FILE_MULTIPART         = false
FILE_MAX_BUFFER        = 65536
```
