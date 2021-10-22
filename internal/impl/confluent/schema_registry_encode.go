package confluent

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/linkedin/goavro/v2"
)

func schemaRegistryEncoderConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		// Stable(). TODO
		Categories("Parsing", "Integration").
		Summary("Automatically encodes and validates messages with schemas from a Confluent Schema Registry service.").
		Description(`
Encodes messages automatically from schemas obtains from a [Confluent Schema Registry service](https://docs.confluent.io/platform/current/schema-registry/index.html) by polling the service for the latest schema version for target subjects.

If a message fails to encode under the schema then it will remain unchanged and the error can be caught using error handling methods outlined [here](/docs/configuration/error_handling).

Currently only Avro schemas are supported.`).
		Field(service.NewStringField("url").Description("The base URL of the schema registry service.")).
		Field(service.NewInterpolatedStringField("subject").Description("The schema subject to derive schemas from.").
			Example("foo").
			Example(`${! meta("kafka_topic") }`)).
		Field(service.NewTLSField("tls")).
		Version("3.58.0")
}

func init() {
	err := service.RegisterProcessor(
		"schema_registry_encode", schemaRegistryEncoderConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			urlStr, err := conf.FieldString("url")
			if err != nil {
				return nil, err
			}
			subject, err := conf.FieldInterpolatedString("subject")
			if err != nil {
				return nil, err
			}
			tlsConf, err := conf.FieldTLS("tls")
			if err != nil {
				return nil, err
			}
			return newSchemaRegistryEncoder(urlStr, tlsConf, subject, mgr.Logger())
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type schemaRegistryEncoder struct {
	client  *http.Client
	subject *service.InterpolatedString

	schemaServerURL *url.URL

	schemas    map[string]*cachedSchemaEncoder
	cacheMut   sync.RWMutex
	requestMut sync.Mutex
	shutSig    *shutdown.Signaller

	logger *service.Logger
	nowFn  func() time.Time
}

func newSchemaRegistryEncoder(urlStr string, tlsConf *tls.Config, subject *service.InterpolatedString, logger *service.Logger) (*schemaRegistryEncoder, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse url: %w", err)
	}

	s := &schemaRegistryEncoder{
		schemaServerURL: u,
		subject:         subject,
		schemas:         map[string]*cachedSchemaEncoder{},
		shutSig:         shutdown.NewSignaller(),
		logger:          logger,
		nowFn:           time.Now,
	}

	s.client = http.DefaultClient
	if tlsConf != nil {
		s.client = &http.Client{}
		if c, ok := http.DefaultTransport.(*http.Transport); ok {
			cloned := c.Clone()
			cloned.TLSClientConfig = tlsConf
			s.client.Transport = cloned
		} else {
			s.client.Transport = &http.Transport{
				TLSClientConfig: tlsConf,
			}
		}
	}

	go func() {
		for {
			select {
			case <-time.After(schemaCacheRefreshPeriod):
				s.refreshEncoders()
			case <-s.shutSig.CloseAtLeisureChan():
				return
			}
		}
	}()
	return s, nil
}

func (s *schemaRegistryEncoder) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	encoder, id, err := s.getEncoder(s.subject.String(msg))
	if err != nil {
		return nil, err
	}

	newMsg := msg.Copy()
	if err := encoder(newMsg); err != nil {
		return nil, err
	}

	rawBytes, err := newMsg.AsBytes()
	if err != nil {
		return nil, errors.New("unable to reference encoded message as bytes")
	}

	if rawBytes, err = insertID(id, rawBytes); err != nil {
		return nil, err
	}
	newMsg.SetBytes(rawBytes)

	return service.MessageBatch{newMsg}, nil
}

func (s *schemaRegistryEncoder) Close(ctx context.Context) error {
	s.shutSig.CloseNow()
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

const (
	schemaRefreshAfter       = time.Minute * 10
	schemaCacheRefreshPeriod = time.Minute
)

func (s *schemaRegistryEncoder) refreshEncoders() {
	// First pass in read only mode to gather purge candidates and refresh
	// candidates
	s.cacheMut.RLock()
	purgeTargetTime := s.nowFn().Add(-schemaStaleAfter).Unix()
	updateTargetTime := s.nowFn().Add(-schemaRefreshAfter).Unix()
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
				s.schemas[k].encoder = encoder
				s.schemas[k].id = id
				s.schemas[k].lastUpdatedUnixSeconds = s.nowFn().Unix()
				s.cacheMut.Unlock()
			}
		}
		s.requestMut.Unlock()
	}
}

func (s *schemaRegistryEncoder) getLatestEncoder(subject string) (schemaEncoder, int, error) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	tmpURL := *s.schemaServerURL
	tmpURL.Path = fmt.Sprintf("/subjects/%s/versions/latest", subject)

	req, err := http.NewRequestWithContext(ctx, "GET", tmpURL.String(), nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json")

	var resBytes []byte
	for i := 0; i < 3; i++ {
		var res *http.Response
		if res, err = s.client.Do(req); err != nil {
			s.logger.Errorf("request failed for schema subject '%v': %v", subject, err)
			continue
		}

		if res.StatusCode == http.StatusNotFound {
			err = fmt.Errorf("schema subject '%v' not found by registry", subject)
			s.logger.Errorf(err.Error())
			break
		}

		if res.StatusCode != http.StatusOK {
			err = fmt.Errorf("request failed for schema subject '%v'", subject)
			s.logger.Errorf(err.Error())
			// TODO: Best attempt at parsing out the body
			continue
		}

		if res.Body == nil {
			s.logger.Errorf("request for schema subject latest '%v' returned an empty body", subject)
			err = errors.New("schema request returned an empty body")
			continue
		}

		resBytes, err = io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			s.logger.Errorf("failed to read response for schema subject '%v': %v", subject, err)
			continue
		}

		break
	}
	if err != nil {
		return nil, 0, err
	}

	resPayload := struct {
		Schema string `json:"schema"`
		ID     int    `json:"id"`
	}{}
	if err = json.Unmarshal(resBytes, &resPayload); err != nil {
		s.logger.Errorf("failed to parse response for schema subject '%v': %v", subject, err)
		return nil, 0, err
	}

	var codec *goavro.Codec
	if codec, err = goavro.NewCodec(resPayload.Schema); err != nil {
		s.logger.Errorf("failed to parse response for schema subject '%v': %v", subject, err)
		return nil, 0, err
	}

	return func(m *service.Message) error {
		datum, err := m.AsStructured()
		if err != nil {
			return err
		}
		binary, err := codec.BinaryFromNative(nil, datum)
		if err != nil {
			return err
		}
		m.SetBytes(binary)
		return nil
	}, resPayload.ID, nil
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
	s.schemas[subject] = &cachedSchemaEncoder{
		lastUsedUnixSeconds:    s.nowFn().Unix(),
		lastUpdatedUnixSeconds: s.nowFn().Unix(),
		id:                     id,
		encoder:                encoder,
	}
	s.cacheMut.Unlock()

	return encoder, id, nil
}
