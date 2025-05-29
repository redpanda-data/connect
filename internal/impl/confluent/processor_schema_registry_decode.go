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

package confluent

import (
	"context"
	"crypto/tls"
	"errors"
	"io/fs"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func schemaRegistryDecoderConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Categories("Parsing", "Integration").
		Summary("Automatically decodes and validates messages with schemas from a Confluent Schema Registry service.").
		Description(`
Decodes messages automatically from a schema stored within a https://docs.confluent.io/platform/current/schema-registry/index.html[Confluent Schema Registry service^] by extracting a schema ID from the message and obtaining the associated schema from the registry. If a message fails to match against the schema then it will remain unchanged and the error can be caught using xref:configuration:error_handling.adoc[error handling methods].

Avro, Protobuf and Json schemas are supported, all are capable of expanding from schema references as of v4.22.0.

== Avro JSON format

This processor creates documents formatted as https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^] when decoding with Avro schemas. In this format the value of a union is encoded in JSON as follows:

- if its type is `+"`null`, then it is encoded as a JSON `null`"+`;
- otherwise it is encoded as a JSON object with one name/value pair whose name is the type's name and whose value is the recursively encoded value. For Avro's named types (record, fixed or enum) the user-specified name is used, for other types the type name is used.

For example, the union schema `+"`[\"null\",\"string\",\"Foo\"]`, where `Foo`"+` is a record name, would encode:

- `+"`null` as `null`"+`;
- the string `+"`\"a\"` as `{\"string\": \"a\"}`"+`; and
- a `+"`Foo` instance as `{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`"+` instance.

However, it is possible to instead create documents in https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard/raw JSON format^] by setting the field `+"<<avro_raw_json, `avro_raw_json`>> to `true`"+`.

== Protobuf format

This processor decodes protobuf messages to JSON documents, you can read more about JSON mapping of protobuf messages here: https://developers.google.com/protocol-buffers/docs/proto3#json

== Metadata

This processor also adds the following metadata to each outgoing message:

schema_id: the ID of the schema in the schema registry that was associated with the message.
`).
		Field(service.NewBoolField("avro_raw_json").
			Description("Whether Avro messages should be decoded into normal JSON (\"json that meets the expectations of regular internet json\") rather than https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^]. If `true` the schema returned from the subject should be decoded as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard json^] instead of as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodec[avro json^]. There is a https://github.com/linkedin/goavro/blob/5ec5a5ee7ec82e16e6e2b438d610e1cab2588393/union.go#L224-L249[comment in goavro^], the https://github.com/linkedin/goavro[underlining library used for avro serialization^], that explains in more detail the difference between the standard json and avro json.").
			Advanced().Default(false).Deprecated()).
		Fields(
			service.NewObjectField(
				"avro",
				service.NewBoolField("raw_unions").Description(`Whether avro messages should be decoded into normal JSON ("json that meets the expectations of regular internet json") rather than https://avro.apache.org/docs/current/specification/_print/#json-encoding[JSON as specified in the Avro Spec^].

For example, if there is a union schema `+"`"+`["null", "string", "Foo"]`+"`"+` where `+"`Foo`"+` is a record name, with raw_unions as false (the default) you get:
- `+"`null` as `null`"+`;
- the string `+"`\"a\"` as `{\"string\": \"a\"}`"+`; and
- a `+"`Foo` instance as `{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`"+` instance.

When raw_unions is set to true then the above union schema is decoded as the following:
- `+"`null` as `null`"+`;
- the string `+"`\"a\"` as `\"a\"`"+`; and
- a `+"`Foo` instance as `{...}`, where `{...}` indicates the JSON encoding of a `Foo`"+` instance.
`).Optional(),
				service.NewBoolField("preserve_logical_types").Description(`Whether logical types should be preserved or transformed back into their primitive type. By default, decimals are decoded as raw bytes and timestamps are decoded as plain integers. Setting this field to true keeps decimal types as numbers in bloblang and timestamps as time values.`).Default(false),
				service.NewBoolField("translate_kafka_connect_types").Description(`Only valid if preserve_logical_types is true. This decodes various Kafka Connect types into their bloblang equivalents when not representable by standard logical types according to the Avro standard.

Types that are currently translated:

.Debezium Custom Temporal Types
|===
|Type Name |Bloblang Type |Description

|io.debezium.time.Date
|timestamp
|Date without time (days since epoch)

|io.debezium.time.Timestamp
|timestamp
|Timestamp without timezone (milliseconds since epoch)

|io.debezium.time.MicroTimestamp
|timestamp
|Timestamp with microsecond precision

|io.debezium.time.NanoTimestamp
|timestamp
|Timestamp with nanosecond precision

|io.debezium.time.ZonedTimestamp
|timestamp
|Timestamp with timezone (ISO-8601 format)

|io.debezium.time.Year
|timestamp at January 1st at 00:00:00
|Year value

|io.debezium.time.Time
|timestamp at the unix epoch
|Time without date (milliseconds past midnight)

|io.debezium.time.MicroTime
|timestamp at the unix epoch
|Time with microsecond precision

|io.debezium.time.NanoTime
|timestamp at the unix epoch
|Time with nanosecond precision

|===

`).Default(false),
				service.NewBloblangField("mapping").Description(`A custom mapping to apply to Avro schemas JSON representation. This is useful to transform custom types emitted by other tools into standard avro.`).
					Optional().
					Advanced().Example(`
map isDebeziumTimestampType {
  root = this.type == "long" && this."connect.name" == "io.debezium.time.Timestamp" && !this.exists("logicalType")
}
map debeziumTimestampToAvroTimestamp {
  let mapped_fields = this.fields.or([]).map_each(item -> item.apply("debeziumTimestampToAvroTimestamp"))
  root = match {
    this.type == "record" => this.assign({"fields": $mapped_fields})
    this.type.type() == "array" => this.assign({"type": this.type.map_each(item -> item.apply("debeziumTimestampToAvroTimestamp"))})
    # Add a logical type so that it's decoded as a timestamp instead of a long.
    this.type.type() == "object" && this.type.apply("isDebeziumTimestampType") => this.merge({"type":{"logicalType": "timestamp-millis"}})
    _ => this
  }
}
root = this.apply("debeziumTimestampToAvroTimestamp")
`),
			).Description("Configuration for how to decode schemas that are of type AVRO."),
			service.NewDurationField("cache_duration").
				Description("The duration after which a schema is considered stale and will be removed from the cache.").
				Default("10m").Example("1h").Example("5m"),
		).
		Field(service.NewURLField("url").Description("The base URL of the schema registry service."))

	for _, f := range service.NewHTTPRequestAuthSignerFields() {
		spec = spec.Field(f.Version("4.7.0"))
	}

	return spec.Field(service.NewTLSField("tls"))
}

func init() {
	service.MustRegisterProcessor(
		"schema_registry_decode", schemaRegistryDecoderConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newSchemaRegistryDecoderFromConfig(conf, mgr)
		})
}

//------------------------------------------------------------------------------

type decodingConfig struct {
	avro struct {
		useHamba                   bool
		rawUnions                  bool
		translateKafkaConnectTypes bool
		mapping                    *bloblang.Executor
	}
}

type schemaRegistryDecoder struct {
	cfg    decodingConfig
	client *sr.Client

	schemas    map[int]*cachedSchemaDecoder
	cacheMut   sync.RWMutex
	requestMut sync.Mutex
	shutSig    *shutdown.Signaller

	mgr    *service.Resources
	logger *service.Logger
}

func newSchemaRegistryDecoderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*schemaRegistryDecoder, error) {
	urlStr, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}
	tlsConf, err := conf.FieldTLS("tls")
	if err != nil {
		return nil, err
	}
	authSigner, err := conf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}
	var cfg decodingConfig
	cfg.avro.rawUnions, err = conf.FieldBool("avro_raw_json")
	if err != nil {
		return nil, err
	}

	cfg.avro.useHamba, err = conf.FieldBool("avro", "preserve_logical_types")
	if err != nil {
		return nil, err
	}
	cfg.avro.translateKafkaConnectTypes, err = conf.FieldBool("avro", "translate_kafka_connect_types")
	if err != nil {
		return nil, err
	}
	if conf.Contains("avro", "raw_unions") {
		cfg.avro.rawUnions, err = conf.FieldBool("avro", "raw_unions")
		if err != nil {
			return nil, err
		}
	}
	if conf.Contains("avro", "mapping") {
		cfg.avro.mapping, err = conf.FieldBloblang("avro", "mapping")
		if err != nil {
			return nil, err
		}
	}
	cacheDuration, err := conf.FieldDuration("cache_duration")
	if err != nil {
		return nil, err
	}
	return newSchemaRegistryDecoder(urlStr, authSigner, tlsConf, cfg, cacheDuration, mgr)
}

func newSchemaRegistryDecoder(
	urlStr string,
	reqSigner func(f fs.FS, req *http.Request) error,
	tlsConf *tls.Config,
	cfg decodingConfig,
	cacheDuration time.Duration,
	mgr *service.Resources,
) (*schemaRegistryDecoder, error) {
	s := &schemaRegistryDecoder{
		cfg:     cfg,
		schemas: map[int]*cachedSchemaDecoder{},
		shutSig: shutdown.NewSignaller(),
		logger:  mgr.Logger(),
		mgr:     mgr,
	}
	var err error
	if s.client, err = sr.NewClient(urlStr, reqSigner, tlsConf, mgr); err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-time.After(schemaCachePurgePeriod):
				s.clearExpired(cacheDuration)
			case <-s.shutSig.SoftStopChan():
				return
			}
		}
	}()
	return s, nil
}

func (s *schemaRegistryDecoder) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	b, err := msg.AsBytes()
	if err != nil {
		return nil, errors.New("unable to reference message as bytes")
	}

	var ch franz_sr.ConfluentHeader
	id, remaining, err := ch.DecodeID(b)
	if err != nil {
		return nil, err
	}

	decoder, err := s.getDecoder(id)
	if err != nil {
		return nil, err
	}

	msg.SetBytes(remaining)
	if err := decoder(msg); err != nil {
		return nil, err
	}
	msg.MetaSetMut("schema_id", id)

	return service.MessageBatch{msg}, nil
}

func (s *schemaRegistryDecoder) Close(ctx context.Context) error {
	s.shutSig.TriggerHardStop()
	s.cacheMut.Lock()
	defer s.cacheMut.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for k := range s.schemas {
		delete(s.schemas, k)
	}
	return nil
}

//------------------------------------------------------------------------------

type schemaDecoder func(m *service.Message) error

type cachedSchemaDecoder struct {
	lastUsedUnixSeconds int64
	decoder             schemaDecoder
}

const (
	schemaStaleAfter       = 10 * time.Minute
	schemaCachePurgePeriod = time.Minute
)

func (s *schemaRegistryDecoder) clearExpired(schemaStaleAfter time.Duration) {
	// First pass in read only mode to gather candidates
	s.cacheMut.RLock()
	targetTime := time.Now().Add(-schemaStaleAfter).Unix()
	var targets []int
	for k, v := range s.schemas {
		if atomic.LoadInt64(&v.lastUsedUnixSeconds) < targetTime {
			targets = append(targets, k)
		}
	}
	s.cacheMut.RUnlock()

	// Second pass fully locks schemas and removes stale decoders
	if len(targets) > 0 {
		s.cacheMut.Lock()
		for _, k := range targets {
			if s.schemas[k].lastUsedUnixSeconds < targetTime {
				delete(s.schemas, k)
			}
		}
		s.cacheMut.Unlock()
	}
}

func (s *schemaRegistryDecoder) getDecoder(id int) (schemaDecoder, error) {
	s.cacheMut.RLock()
	c, ok := s.schemas[id]
	s.cacheMut.RUnlock()
	if ok {
		atomic.StoreInt64(&c.lastUsedUnixSeconds, time.Now().Unix())
		return c.decoder, nil
	}

	s.requestMut.Lock()
	defer s.requestMut.Unlock()

	// We might've been beaten to making the request, so check once more whilst
	// within the request lock.
	s.cacheMut.RLock()
	c, ok = s.schemas[id]
	s.cacheMut.RUnlock()
	if ok {
		atomic.StoreInt64(&c.lastUsedUnixSeconds, time.Now().Unix())
		return c.decoder, nil
	}

	// TODO: Expose this via configuration
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	resPayload, err := s.client.GetSchemaByID(ctx, id, false)
	if err != nil {
		return nil, err
	}

	var decoder schemaDecoder
	switch resPayload.Type {
	case franz_sr.TypeProtobuf:
		decoder, err = s.getProtobufDecoder(ctx, resPayload)
	case franz_sr.TypeJSON:
		decoder, err = s.getJSONDecoder(ctx, resPayload)
	default:
		if s.cfg.avro.useHamba {
			decoder, err = s.getHambaAvroDecoder(ctx, resPayload)
		} else {
			decoder, err = s.getGoAvroDecoder(ctx, resPayload)
		}
	}
	if err != nil {
		return nil, err
	}

	s.cacheMut.Lock()
	s.schemas[id] = &cachedSchemaDecoder{
		lastUsedUnixSeconds: time.Now().Unix(),
		decoder:             decoder,
	}
	s.cacheMut.Unlock()

	return decoder, nil
}
