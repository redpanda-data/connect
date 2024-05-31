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

	"github.com/redpanda-data/benthos/v4/public/service"
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

- if its type is ` + "`null`, then it is encoded as a JSON `null`" + `;
- otherwise it is encoded as a JSON object with one name/value pair whose name is the type's name and whose value is the recursively encoded value. For Avro's named types (record, fixed or enum) the user-specified name is used, for other types the type name is used.

For example, the union schema ` + "`[\"null\",\"string\",\"Foo\"]`, where `Foo`" + ` is a record name, would encode:

- ` + "`null` as `null`" + `;
- the string ` + "`\"a\"` as `\\{\"string\": \"a\"}`" + `; and
- a ` + "`Foo` instance as `\\{\"Foo\": {...}}`, where `{...}` indicates the JSON encoding of a `Foo`" + ` instance.

However, it is possible to instead create documents in https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard/raw JSON format^] by setting the field ` + "<<avro_raw_json, `avro_raw_json`>> to `true`" + `.

== Protobuf format

This processor decodes protobuf messages to JSON documents, you can read more about JSON mapping of protobuf messages here: https://developers.google.com/protocol-buffers/docs/proto3#json
`).
		Field(service.NewBoolField("avro_raw_json").
			Description("Whether Avro messages should be decoded into normal JSON (\"json that meets the expectations of regular internet json\") rather than https://avro.apache.org/docs/current/specification/_print/#json-encoding[Avro JSON^]. If `true` the schema returned from the subject should be decoded as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodecForStandardJSONFull[standard json^] instead of as https://pkg.go.dev/github.com/linkedin/goavro/v2#NewCodec[avro json^]. There is a https://github.com/linkedin/goavro/blob/5ec5a5ee7ec82e16e6e2b438d610e1cab2588393/union.go#L224-L249[comment in goavro^], the https://github.com/linkedin/goavro[underlining library used for avro serialization^], that explains in more detail the difference between the standard json and avro json.").
			Advanced().Default(false)).
		Field(service.NewURLField("url").Description("The base URL of the schema registry service."))

	for _, f := range service.NewHTTPRequestAuthSignerFields() {
		spec = spec.Field(f.Version("4.7.0"))
	}

	return spec.Field(service.NewTLSField("tls"))
}

func init() {
	err := service.RegisterProcessor(
		"schema_registry_decode", schemaRegistryDecoderConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newSchemaRegistryDecoderFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type schemaRegistryDecoder struct {
	avroRawJSON bool
	client      *schemaRegistryClient

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
	avroRawJSON, err := conf.FieldBool("avro_raw_json")
	if err != nil {
		return nil, err
	}
	return newSchemaRegistryDecoder(urlStr, authSigner, tlsConf, avroRawJSON, mgr)
}

func newSchemaRegistryDecoder(
	urlStr string,
	reqSigner func(f fs.FS, req *http.Request) error,
	tlsConf *tls.Config,
	avroRawJSON bool,
	mgr *service.Resources,
) (*schemaRegistryDecoder, error) {
	s := &schemaRegistryDecoder{
		avroRawJSON: avroRawJSON,
		schemas:     map[int]*cachedSchemaDecoder{},
		shutSig:     shutdown.NewSignaller(),
		logger:      mgr.Logger(),
		mgr:         mgr,
	}
	var err error
	if s.client, err = newSchemaRegistryClient(urlStr, reqSigner, tlsConf, mgr); err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-time.After(schemaCachePurgePeriod):
				s.clearExpired()
			case <-s.shutSig.SoftStopChan():
				return
			}
		}
	}()
	return s, nil
}

func (s *schemaRegistryDecoder) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	b, err := msg.AsBytes()
	if err != nil {
		return nil, errors.New("unable to reference message as bytes")
	}

	id, remaining, err := extractID(b)
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

func extractID(b []byte) (id int, remaining []byte, err error) {
	if len(b) == 0 {
		err = errors.New("message is empty")
		return
	}
	if b[0] != 0 {
		err = fmt.Errorf("serialization format version number %v not supported", b[0])
		return
	}
	id = int(binary.BigEndian.Uint32(b[1:5]))
	remaining = b[5:]
	return
}

const (
	schemaStaleAfter       = time.Minute * 10
	schemaCachePurgePeriod = time.Minute
)

func (s *schemaRegistryDecoder) clearExpired() {
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

	resPayload, err := s.client.GetSchemaByID(ctx, id)
	if err != nil {
		return nil, err
	}

	var decoder schemaDecoder
	switch resPayload.Type {
	case "PROTOBUF":
		decoder, err = s.getProtobufDecoder(ctx, resPayload)
	case "", "AVRO":
		decoder, err = s.getAvroDecoder(ctx, resPayload)
	case "JSON":
		decoder, err = s.getJSONDecoder(ctx, resPayload)
	default:
		err = fmt.Errorf("schema type %v not supported", resPayload.Type)
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
