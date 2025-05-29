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
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	franz_sr "github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

func schemaRegistryEncoderConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Version("3.58.0").
		Categories("Parsing", "Integration").
		Summary("Automatically encodes and validates messages with schemas from a Confluent Schema Registry service.").
		Description(`
Encodes messages automatically from schemas obtains from a https://docs.confluent.io/platform/current/schema-registry/index.html[Confluent Schema Registry service^] by polling the service for the latest schema version for target subjects.

If a message fails to encode under the schema then it will remain unchanged and the error can be caught using xref:configuration:error_handling.adoc[error handling methods].

Avro, Protobuf and Json schemas are supported, all are capable of expanding from schema references as of v4.22.0.

== Avro JSON format

By default this processor expects documents formatted as https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^] when encoding with Avro schemas. In this format the value of a union is encoded in JSON as follows:

- if its type is ` + "`null`, then it is encoded as a JSON `null`" + `;
- otherwise it is encoded as a JSON object with one name/value pair whose name is the type's name and whose value is the recursively encoded value. For Avro's named types (record, fixed or enum) the user-specified name is used, for other types the type name is used.

For example, the union schema ` + "`[\"null\",\"string\",\"Foo\"]`, where `Foo`" + ` is a record name, would encode:

- ` + "`null` as `null`" + `;
- the string ` + "`\"a\"` as `\\{\"string\": \"a\"}`" + `; and
- a ` + "`Foo` instance as `\\{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`" + ` instance.

However, it is possible to instead consume documents in https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard/raw JSON format^] by setting the field ` + "<<avro_raw_json, `avro_raw_json`>> to `true`" + `.

=== Known issues

Important! There is an outstanding issue in the https://github.com/linkedin/goavro[avro serializing library^] that Redpanda Connect uses which means it https://github.com/linkedin/goavro/issues/252[doesn't encode logical types correctly^]. It's still possible to encode logical types that are in-line with the spec if ` + "`avro_raw_json` is set to true" + `, though now of course non-logical types will not be in-line with the spec.

== Protobuf format

This processor encodes protobuf messages either from any format parsed within Redpanda Connect (encoded as JSON by default), or from raw JSON documents, you can read more about JSON mapping of protobuf messages here: https://developers.google.com/protocol-buffers/docs/proto3#json

=== Multiple message support

When a target subject presents a protobuf schema that contains multiple messages it becomes ambiguous which message definition a given input data should be encoded against. In such scenarios Redpanda Connect will attempt to encode the data against each of them and select the first to successfully match against the data, this process currently *ignores all nested message definitions*. In order to speed up this exhaustive search the last known successful message will be attempted first for each subsequent input.

We will be considering alternative approaches in future so please https://redpanda.com/slack[get in touch^] with thoughts and feedback.
`).
		Field(service.NewURLField("url").Description("The base URL of the schema registry service.")).
		Field(service.NewInterpolatedStringField("subject").Description("The schema subject to derive schemas from.").
			Example("foo").
			Example(`${! meta("kafka_topic") }`)).
		Field(service.NewStringField("refresh_period").
			Description("The period after which a schema is refreshed for each subject, this is done by polling the schema registry service.").
			Default("10m").
			Example("60s").
			Example("1h")).
		Field(service.NewBoolField("avro_raw_json").
			Description("Whether messages encoded in Avro format should be parsed as normal JSON (\"json that meets the expectations of regular internet json\") rather than https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^]. If `true` the schema returned from the subject should be parsed as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard json^] instead of as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodec[avro json^]. There is a https://github.com/linkedin/goavro/blob/5ec5a5ee7ec82e16e6e2b438d610e1cab2588393/union.go#L224-L249[comment in goavro^], the https://github.com/linkedin/goavro[underlining library used for avro serialization^], that explains in more detail the difference between standard json and avro json.").
			Advanced().Default(false).Version("3.59.0"))

	for _, f := range service.NewHTTPRequestAuthSignerFields() {
		spec = spec.Field(f.Version("4.7.0"))
	}

	return spec.Field(service.NewTLSField("tls"))
}

func init() {
	service.MustRegisterBatchProcessor(
		"schema_registry_encode", schemaRegistryEncoderConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newSchemaRegistryEncoderFromConfig(conf, mgr)
		})
}

//------------------------------------------------------------------------------

type schemaRegistryEncoder struct {
	client             *sr.Client
	subject            *service.InterpolatedString
	avroRawJSON        bool
	schemaRefreshAfter time.Duration

	schemas    map[string]cachedSchemaEncoder
	cacheMut   sync.RWMutex
	requestMut sync.Mutex
	shutSig    *shutdown.Signaller

	logger *service.Logger
	mgr    *service.Resources
	nowFn  func() time.Time
}

func newSchemaRegistryEncoderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*schemaRegistryEncoder, error) {
	urlStr, err := conf.FieldString("url")
	if err != nil {
		return nil, err
	}
	subject, err := conf.FieldInterpolatedString("subject")
	if err != nil {
		return nil, err
	}
	avroRawJSON, err := conf.FieldBool("avro_raw_json")
	if err != nil {
		return nil, err
	}
	refreshPeriodStr, err := conf.FieldString("refresh_period")
	if err != nil {
		return nil, err
	}
	refreshPeriod, err := time.ParseDuration(refreshPeriodStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse refresh period: %v", err)
	}
	refreshTicker := refreshPeriod / 10
	if refreshTicker < time.Second {
		refreshTicker = time.Second
	}
	authSigner, err := conf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}
	tlsConf, err := conf.FieldTLS("tls")
	if err != nil {
		return nil, err
	}
	return newSchemaRegistryEncoder(urlStr, authSigner, tlsConf, subject, avroRawJSON, refreshPeriod, refreshTicker, mgr)
}

func newSchemaRegistryEncoder(
	urlStr string,
	reqSigner func(f fs.FS, req *http.Request) error,
	tlsConf *tls.Config,
	subject *service.InterpolatedString,
	avroRawJSON bool,
	schemaRefreshAfter, schemaRefreshTicker time.Duration,
	mgr *service.Resources,
) (*schemaRegistryEncoder, error) {
	s := &schemaRegistryEncoder{
		subject:            subject,
		avroRawJSON:        avroRawJSON,
		schemaRefreshAfter: schemaRefreshAfter,
		schemas:            map[string]cachedSchemaEncoder{},
		shutSig:            shutdown.NewSignaller(),
		logger:             mgr.Logger(),
		mgr:                mgr,
		nowFn:              time.Now,
	}
	var err error
	if s.client, err = sr.NewClient(urlStr, reqSigner, tlsConf, mgr); err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-time.After(schemaRefreshTicker):
				s.refreshEncoders()
			case <-s.shutSig.SoftStopChan():
				return
			}
		}
	}()
	return s, nil
}

func (s *schemaRegistryEncoder) ProcessBatch(_ context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batch = batch.Copy()
	for i, msg := range batch {
		subject, err := batch.TryInterpolatedString(i, s.subject)
		if err != nil {
			s.logger.Errorf("Subject interpolation error: %v", err)
			msg.SetError(fmt.Errorf("subject interpolation error: %w", err))
			continue
		}

		encoder, id, err := s.getEncoder(subject)
		if err != nil {
			msg.SetError(err)
			continue
		}

		if err := encoder(msg); err != nil {
			msg.SetError(err)
			continue
		}

		rawBytes, err := msg.AsBytes()
		if err != nil {
			msg.SetError(errors.New("unable to reference encoded message as bytes"))
			continue
		}

		if rawBytes, err = insertID(id, rawBytes); err != nil {
			msg.SetError(err)
			continue
		}
		msg.SetBytes(rawBytes)
	}
	return []service.MessageBatch{batch}, nil
}

func (s *schemaRegistryEncoder) Close(ctx context.Context) error {
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

type schemaEncoder func(m *service.Message) error

type cachedSchemaEncoder struct {
	lastUsedUnixSeconds    int64
	lastUpdatedUnixSeconds int64
	id                     int
	encoder                schemaEncoder
}

func insertID(id int, content []byte) ([]byte, error) {
	newBytes := make([]byte, len(content)+5)

	binary.BigEndian.PutUint32(newBytes[1:], uint32(id))
	copy(newBytes[5:], content)

	return newBytes, nil
}

func (s *schemaRegistryEncoder) refreshEncoders() {
	// First pass in read only mode to gather purge candidates and refresh
	// candidates
	s.cacheMut.RLock()
	purgeTargetTime := s.nowFn().Add(-schemaStaleAfter).Unix()
	updateTargetTime := s.nowFn().Add(-s.schemaRefreshAfter).Unix()
	var purgeTargets, refreshTargets []string
	for k, v := range s.schemas {
		if atomic.LoadInt64(&v.lastUsedUnixSeconds) < purgeTargetTime {
			purgeTargets = append(purgeTargets, k)
		} else if atomic.LoadInt64(&v.lastUpdatedUnixSeconds) < updateTargetTime {
			refreshTargets = append(refreshTargets, k)
		}
	}
	s.cacheMut.RUnlock()

	// Second pass fully locks schemas and removes stale decoders
	if len(purgeTargets) > 0 {
		s.cacheMut.Lock()
		for _, k := range purgeTargets {
			if s.schemas[k].lastUsedUnixSeconds < purgeTargetTime {
				delete(s.schemas, k)
			}
		}
		s.cacheMut.Unlock()
	}

	// Each refresh target gets updated passively
	if len(refreshTargets) > 0 {
		s.requestMut.Lock()
		for _, k := range refreshTargets {
			encoder, id, err := s.getLatestEncoder(k)
			if err != nil {
				s.logger.Errorf("Failed to refresh schema subject '%v': %v", k, err)
			} else {
				s.cacheMut.Lock()
				s.schemas[k] = cachedSchemaEncoder{
					encoder:                encoder,
					id:                     id,
					lastUpdatedUnixSeconds: s.nowFn().Unix(),
					lastUsedUnixSeconds:    s.schemas[k].lastUsedUnixSeconds,
				}
				s.cacheMut.Unlock()
			}
		}
		s.requestMut.Unlock()
	}
}

func (s *schemaRegistryEncoder) getLatestEncoder(subject string) (schemaEncoder, int, error) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	resPayload, err := s.client.GetSchemaBySubjectAndVersion(ctx, subject, nil, false)
	if err != nil {
		return nil, 0, err
	}

	s.logger.Tracef("Loaded new codec for subject %v: %s", subject, resPayload.Schema)

	var encoder schemaEncoder
	switch resPayload.Type {
	case franz_sr.TypeProtobuf:
		encoder, err = s.getProtobufEncoder(ctx, resPayload.Schema)
	case franz_sr.TypeJSON:
		encoder, err = s.getJSONEncoder(ctx, resPayload.Schema)
	default:
		encoder, err = s.getAvroEncoder(ctx, resPayload.Schema)
	}
	if err != nil {
		return nil, 0, err
	}

	return encoder, resPayload.ID, nil
}

func (s *schemaRegistryEncoder) getEncoder(subject string) (schemaEncoder, int, error) {
	s.cacheMut.RLock()
	c, ok := s.schemas[subject]
	s.cacheMut.RUnlock()
	if ok {
		atomic.StoreInt64(&c.lastUsedUnixSeconds, s.nowFn().Unix())
		return c.encoder, c.id, nil
	}

	s.requestMut.Lock()
	defer s.requestMut.Unlock()

	// We might've been beaten to making the request, so check once more whilst
	// within the request lock.
	s.cacheMut.RLock()
	c, ok = s.schemas[subject]
	s.cacheMut.RUnlock()
	if ok {
		atomic.StoreInt64(&c.lastUsedUnixSeconds, s.nowFn().Unix())
		return c.encoder, c.id, nil
	}

	encoder, id, err := s.getLatestEncoder(subject)
	if err != nil {
		return nil, 0, err
	}

	s.cacheMut.Lock()
	s.schemas[subject] = cachedSchemaEncoder{
		lastUsedUnixSeconds:    s.nowFn().Unix(),
		lastUpdatedUnixSeconds: s.nowFn().Unix(),
		id:                     id,
		encoder:                encoder,
	}
	s.cacheMut.Unlock()

	return encoder, id, nil
}
