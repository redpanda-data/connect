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
	"github.com/linkedin/goavro/v2"
	franz_sr "github.com/twmb/franz-go/pkg/sr"
	"github.com/xeipuuv/gojsonschema"

	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
)

const (
	sreFieldSchemaMeta     = "schema_metadata"
	sreFieldFormat         = "format"
	sreFieldNormalize      = "normalize"
	sreFieldAvro           = "avro"
	sreFieldAvroRawJSON    = "raw_json"
	sreFieldAvroRecordName = "record_name"
	sreFieldAvroNamespace  = "namespace"
)

func schemaRegistryEncoderConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Version("3.58.0").
		Categories("Parsing", "Integration").
		Summary("Automatically encodes and validates messages with schemas from a Confluent Schema Registry service.").
		Description(`
Encodes messages automatically from schemas obtained from a https://docs.confluent.io/platform/current/schema-registry/index.html[Confluent Schema Registry service^] by polling the service for the latest schema version for target subjects.

Alternatively, when ` + "`schema_metadata`" + ` is set, the processor reads a schema in benthos common schema format from message metadata (as produced by CDC inputs such as ` + "`postgresql`" + `, ` + "`mysql_cdc`" + `, and ` + "`microsoft_sql_server_cdc`" + `), converts it to the target ` + "`format`" + ` (Avro or JSON Schema), registers it with the schema registry, and encodes the message. This is useful when the schema is not pre-registered in the registry and instead travels with the data.

If a message fails to encode under the schema then it will remain unchanged and the error can be caught using xref:configuration:error_handling.adoc[error handling methods].

Avro, Protobuf and JSON Schema formats are supported. In registry-pull mode all three are auto-detected from the registry. In metadata mode Avro and JSON Schema are supported, with the target format selected via the ` + "`format`" + ` field. Schema references are supported in registry-pull mode as of v4.22.0.

== Avro JSON format

By default this processor expects documents formatted as https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^] when encoding with Avro schemas. In this format the value of a union is encoded in JSON as follows:

- if its type is ` + "`null`, then it is encoded as a JSON `null`" + `;
- otherwise it is encoded as a JSON object with one name/value pair whose name is the type's name and whose value is the recursively encoded value. For Avro's named types (record, fixed or enum) the user-specified name is used, for other types the type name is used.

For example, the union schema ` + "`[\"null\",\"string\",\"Foo\"]`, where `Foo`" + ` is a record name, would encode:

- ` + "`null` as `null`" + `;
- the string ` + "`\"a\"` as `\\{\"string\": \"a\"}`" + `; and
- a ` + "`Foo` instance as `\\{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`" + ` instance.

However, it is possible to instead consume documents in https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard/raw JSON format^] by setting ` + "`avro.raw_json`" + ` to ` + "`true`" + `. This is strongly recommended when using ` + "`schema_metadata`" + ` mode, as CDC sources emit standard JSON rather than Avro JSON.

NOTE: The top-level ` + "`avro_raw_json`" + ` field is deprecated in favor of ` + "`avro.raw_json`" + `.

=== Known issues

Important! There is an outstanding issue in the https://github.com/linkedin/goavro[avro serializing library^] that Redpanda Connect uses which means it https://github.com/linkedin/goavro/issues/252[doesn't encode logical types correctly^]. It's still possible to encode logical types that are in-line with the spec if ` + "`avro.raw_json` is set to true" + `, though now of course non-logical types will not be in-line with the spec.

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
			Description("DEPRECATED: Use avro.raw_json instead.").
			Advanced().Default(false).Version("3.59.0").Deprecated()).
		Field(service.NewStringField(sreFieldSchemaMeta).
			Description("When set, the processor reads a schema in benthos common schema format from this metadata key on each message, converts it to the format specified by `format`, registers it with the schema registry under the configured subject, and encodes the message. When empty (the default), the processor pulls the latest schema from the registry instead.").
			Default("")).
		Field(service.NewStringEnumField(sreFieldFormat, "avro", "json_schema").
			Description("The encoding format to use when converting a common schema from metadata. Required when `schema_metadata` is set.").
			Optional()).
		Field(service.NewBoolField(sreFieldNormalize).
			Description("Whether to normalize the schema before registering with the schema registry (schema_metadata mode only).").
			Advanced().Default(true))

	spec = spec.Fields(
		service.NewObjectField(sreFieldAvro,
			service.NewBoolField(sreFieldAvroRawJSON).
				Description("Whether messages encoded in Avro format should be parsed as normal JSON rather than Avro JSON. Overrides the deprecated top-level `avro_raw_json` when set.").
				Optional(),
			service.NewStringField(sreFieldAvroRecordName).
				Description("The name to use for the root Avro record type when encoding from a common schema (schema_metadata mode). If empty, derived from the subject.").
				Default("").Optional(),
			service.NewStringField(sreFieldAvroNamespace).
				Description("The Avro namespace for the root record type when encoding from a common schema (schema_metadata mode).").
				Default("").Optional(),
		).Description("Configuration for Avro encoding."),
	)

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

	// Registry-pull mode cache.
	schemas    map[string]cachedSchemaEncoder
	cacheMut   sync.RWMutex
	requestMut sync.Mutex

	// Metadata-push mode fields.
	schemaMeta     string // metadata key; empty = registry-pull mode
	format         string // "avro" or "json_schema"
	normalize      bool
	recordName     string
	namespace      string
	metaEncoders   map[string]cachedSchemaEncoder
	metaCacheMut   sync.RWMutex
	metaRequestMut sync.Mutex

	shutSig *shutdown.Signaller
	logger  *service.Logger
	mgr     *service.Resources
	nowFn   func() time.Time
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
	// Deprecated top-level field read first, then override with avro.raw_json if set.
	avroRawJSON, err := conf.FieldBool("avro_raw_json")
	if err != nil {
		return nil, err
	}
	if conf.Contains(sreFieldAvro, sreFieldAvroRawJSON) {
		avroRawJSON, err = conf.FieldBool(sreFieldAvro, sreFieldAvroRawJSON)
		if err != nil {
			return nil, err
		}
	}
	refreshPeriodStr, err := conf.FieldString("refresh_period")
	if err != nil {
		return nil, err
	}
	refreshPeriod, err := time.ParseDuration(refreshPeriodStr)
	if err != nil {
		return nil, fmt.Errorf("parsing refresh period: %v", err)
	}
	refreshTicker := max(refreshPeriod/10, time.Second)
	authSigner, err := conf.HTTPRequestAuthSignerFromParsed()
	if err != nil {
		return nil, err
	}
	tlsConf, err := conf.FieldTLS("tls")
	if err != nil {
		return nil, err
	}

	// Parse metadata-mode fields.
	schemaMeta, err := conf.FieldString(sreFieldSchemaMeta)
	if err != nil {
		return nil, err
	}
	var format string
	if conf.Contains(sreFieldFormat) {
		if format, err = conf.FieldString(sreFieldFormat); err != nil {
			return nil, err
		}
	}
	normalize, err := conf.FieldBool(sreFieldNormalize)
	if err != nil {
		return nil, err
	}
	var recordName, namespace string
	if conf.Contains(sreFieldAvro, sreFieldAvroRecordName) {
		recordName, _ = conf.FieldString(sreFieldAvro, sreFieldAvroRecordName)
	}
	if conf.Contains(sreFieldAvro, sreFieldAvroNamespace) {
		namespace, _ = conf.FieldString(sreFieldAvro, sreFieldAvroNamespace)
	}

	// Cross-validate: schema_metadata and format must be set together.
	if schemaMeta != "" && format == "" {
		return nil, errors.New("format is required when schema_metadata is set")
	}
	if schemaMeta == "" && format != "" {
		return nil, errors.New("format is only used when schema_metadata is set")
	}

	// Avro format in metadata mode requires explicit raw_json. We can only
	// reliably detect explicit setting via the new avro.raw_json field (which
	// is Optional with no default). The deprecated avro_raw_json has
	// Default(false) so conf.Contains always returns true for it.
	if schemaMeta != "" && format == "avro" && !conf.Contains(sreFieldAvro, sreFieldAvroRawJSON) {
		return nil, errors.New(
			"schema_metadata mode requires avro.raw_json to be explicitly set; " +
				"CDC sources emit standard JSON so avro.raw_json should typically " +
				"be set to true; set it to false only if your data is already in " +
				"Avro JSON union format")
	}

	s, err := newSchemaRegistryEncoder(urlStr, authSigner, tlsConf, subject, avroRawJSON, refreshPeriod, refreshTicker, mgr)
	if err != nil {
		return nil, err
	}
	s.schemaMeta = schemaMeta
	s.format = format
	s.normalize = normalize
	s.recordName = recordName
	s.namespace = namespace
	if schemaMeta != "" {
		s.metaEncoders = map[string]cachedSchemaEncoder{}
		// Start the metadata-mode purge goroutine. The registry-pull refresh
		// goroutine was already started by newSchemaRegistryEncoder; stop it
		// and replace with the purge-only loop.
		s.shutSig.TriggerSoftStop()
		s.shutSig = shutdown.NewSignaller()
		go func() {
			for {
				select {
				case <-time.After(schemaCachePurgePeriod):
					s.purgeStaleMetaEncoders()
				case <-s.shutSig.SoftStopChan():
					return
				}
			}
		}()
	}
	return s, nil
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

func (s *schemaRegistryEncoder) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if s.schemaMeta != "" {
		return s.processBatchFromMetadata(ctx, batch)
	}
	return s.processBatchFromRegistry(batch)
}

func (s *schemaRegistryEncoder) processBatchFromRegistry(batch service.MessageBatch) ([]service.MessageBatch, error) {
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

func (s *schemaRegistryEncoder) processBatchFromMetadata(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	batch = batch.Copy()
	for i, msg := range batch {
		metaAny, exists := msg.MetaGetMut(s.schemaMeta)
		if !exists {
			msg.SetError(fmt.Errorf("schema metadata key %q not found on message", s.schemaMeta))
			continue
		}

		subject, err := batch.TryInterpolatedString(i, s.subject)
		if err != nil {
			msg.SetError(fmt.Errorf("subject interpolation error: %w", err))
			continue
		}

		encoder, id, err := s.getOrCreateMetaEncoder(ctx, metaAny, subject)
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
	for k := range s.schemas {
		delete(s.schemas, k)
	}
	s.cacheMut.Unlock()

	s.metaCacheMut.Lock()
	for k := range s.metaEncoders {
		delete(s.metaEncoders, k)
	}
	s.metaCacheMut.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
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

	s.logger.Tracef("Loaded new codec for subject %v: %v", subject, resPayload.Schema)

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

//------------------------------------------------------------------------------
// Metadata-mode methods
//------------------------------------------------------------------------------

func (s *schemaRegistryEncoder) getOrCreateMetaEncoder(ctx context.Context, metaAny any, subject string) (schemaEncoder, int, error) {
	fingerprint, err := extractFingerprint(metaAny)
	if err != nil {
		return nil, 0, fmt.Errorf("extracting schema fingerprint: %w", err)
	}

	cacheKey := subject + ":" + fingerprint

	s.metaCacheMut.RLock()
	c, ok := s.metaEncoders[cacheKey]
	s.metaCacheMut.RUnlock()
	if ok {
		atomic.StoreInt64(&c.lastUsedUnixSeconds, s.nowFn().Unix())
		return c.encoder, c.id, nil
	}

	s.metaRequestMut.Lock()
	defer s.metaRequestMut.Unlock()

	// Double-check after acquiring lock.
	s.metaCacheMut.RLock()
	c, ok = s.metaEncoders[cacheKey]
	s.metaCacheMut.RUnlock()
	if ok {
		atomic.StoreInt64(&c.lastUsedUnixSeconds, s.nowFn().Unix())
		return c.encoder, c.id, nil
	}

	common, err := schema.ParseFromAny(metaAny)
	if err != nil {
		return nil, 0, fmt.Errorf("parsing common schema from metadata: %w", err)
	}

	var schemaStr string
	var schemaType franz_sr.SchemaType
	var encoder schemaEncoder

	switch s.format {
	case "avro":
		recordName := s.recordName
		if recordName == "" {
			recordName = sanitizeAvroName(subject)
		}
		avroJSON, aErr := commonToAvroSchema(common, recordName, s.namespace)
		if aErr != nil {
			return nil, 0, fmt.Errorf("converting common schema to Avro: %w", aErr)
		}
		schemaStr = avroJSON
		schemaType = franz_sr.TypeAvro

		var codec *goavro.Codec
		if s.avroRawJSON {
			codec, err = goavro.NewCodecForStandardJSONFull(avroJSON)
		} else {
			codec, err = goavro.NewCodec(avroJSON)
		}
		if err != nil {
			return nil, 0, fmt.Errorf("creating Avro codec: %w", err)
		}
		encoder = func(m *service.Message) error {
			b, bErr := m.AsBytes()
			if bErr != nil {
				return bErr
			}
			native, _, nErr := codec.NativeFromTextual(b)
			if nErr != nil {
				return nErr
			}
			binary, binErr := codec.BinaryFromNative(nil, native)
			if binErr != nil {
				return binErr
			}
			m.SetBytes(binary)
			return nil
		}

	case "json_schema":
		jsonSchemaStr, jErr := commonToJSONSchema(common)
		if jErr != nil {
			return nil, 0, fmt.Errorf("converting common schema to JSON Schema: %w", jErr)
		}
		schemaStr = jsonSchemaStr
		schemaType = franz_sr.TypeJSON

		sch, compileErr := gojsonschema.NewSchema(gojsonschema.NewStringLoader(jsonSchemaStr))
		if compileErr != nil {
			return nil, 0, fmt.Errorf("compiling JSON Schema: %w", compileErr)
		}
		encoder = func(m *service.Message) error {
			b, bErr := m.AsBytes()
			if bErr != nil {
				return bErr
			}
			res, vErr := sch.Validate(gojsonschema.NewBytesLoader(b))
			if vErr != nil {
				return vErr
			}
			if !res.Valid() {
				return fmt.Errorf("json message does not conform to schema: %v", res.Errors())
			}
			return nil
		}

	default:
		return nil, 0, fmt.Errorf("unsupported format: %s", s.format)
	}

	schemaID, err := s.client.CreateSchema(ctx, subject, franz_sr.Schema{
		Schema: schemaStr,
		Type:   schemaType,
	}, s.normalize)
	if err != nil {
		return nil, 0, fmt.Errorf("registering schema for subject %q: %w", subject, err)
	}

	s.metaCacheMut.Lock()
	s.metaEncoders[cacheKey] = cachedSchemaEncoder{
		lastUsedUnixSeconds:    s.nowFn().Unix(),
		lastUpdatedUnixSeconds: s.nowFn().Unix(),
		id:                     schemaID,
		encoder:                encoder,
	}
	s.metaCacheMut.Unlock()

	s.logger.Debugf("Registered schema for subject %q (ID: %d, fingerprint: %s)", subject, schemaID, fingerprint)
	return encoder, schemaID, nil
}

func (s *schemaRegistryEncoder) purgeStaleMetaEncoders() {
	s.metaCacheMut.RLock()
	purgeTargetTime := s.nowFn().Add(-schemaStaleAfter).Unix()
	var purgeTargets []string
	for k, v := range s.metaEncoders {
		if atomic.LoadInt64(&v.lastUsedUnixSeconds) < purgeTargetTime {
			purgeTargets = append(purgeTargets, k)
		}
	}
	s.metaCacheMut.RUnlock()

	if len(purgeTargets) > 0 {
		s.metaCacheMut.Lock()
		for _, k := range purgeTargets {
			if s.metaEncoders[k].lastUsedUnixSeconds < purgeTargetTime {
				delete(s.metaEncoders, k)
			}
		}
		s.metaCacheMut.Unlock()
	}
}

func extractFingerprint(metaAny any) (string, error) {
	m, ok := metaAny.(map[string]any)
	if !ok {
		return "", fmt.Errorf("expected map[string]any, got %T", metaAny)
	}
	fp, ok := m["fingerprint"].(string)
	if !ok {
		return "", errors.New("missing or invalid fingerprint in schema metadata")
	}
	return fp, nil
}
