// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otlp

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	rpotel "github.com/redpanda-data/common-go/redpanda-otel-exporter"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/schemaregistry"
)

// Common field names shared by HTTP and gRPC inputs.
const (
	fieldEncoding  = "encoding"
	fieldRateLimit = "rate_limit"
)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type otlpInput struct {
	log       *service.Logger
	mgr       *service.Resources
	encoding  Encoding
	rateLimit string
	resCh     chan asyncMessage
	shutSig   *shutdown.Signaller

	// Schema Registry fields
	srClient      *sr.Client
	schemaID      map[SignalType]int
	subject       map[SignalType]string
	commonSubject string
}

func newOTLPInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (otlpInput, error) {
	o := otlpInput{
		log:     mgr.Logger(),
		mgr:     mgr,
		resCh:   make(chan asyncMessage),
		shutSig: shutdown.NewSignaller(),
		subject: make(map[SignalType]string),
	}

	// Parse encoding
	es, err := pConf.FieldString(fieldEncoding)
	if err != nil {
		return otlpInput{}, err
	}
	o.encoding = Encoding(es)

	// Parse rate limit
	if o.rateLimit, err = pConf.FieldString(fieldRateLimit); err != nil {
		return otlpInput{}, err
	}

	// Create Schema Registry client if configured
	if o.srClient, err = schemaregistry.ClientFromParsedOptional(pConf, schemaRegistryField, mgr); err != nil {
		return otlpInput{}, fmt.Errorf("create schema registry client: %w", err)
	}

	// Parse subject names or use defaults
	if pConf.Contains(schemaRegistryField) {
		srConf := pConf.Namespace(schemaRegistryField)

		if o.encoding == EncodingProtobuf {
			if o.commonSubject, err = srConf.FieldString(srFieldCommonSubject); err != nil {
				return otlpInput{}, err
			}
			if o.commonSubject == "" {
				o.commonSubject = defaultCommonSubject(o.encoding)
			}
		}
		{
			subj, err := srConf.FieldString(srFieldTraceSubject)
			if err != nil {
				return otlpInput{}, err
			}
			if subj == "" {
				subj = defaultSubject(SignalTypeTrace, o.encoding)
			}
			o.subject[SignalTypeTrace] = subj
		}
		{
			subj, err := srConf.FieldString(srFieldLogSubject)
			if err != nil {
				return otlpInput{}, err
			}
			if subj == "" {
				subj = defaultSubject(SignalTypeLog, o.encoding)
			}
			o.subject[SignalTypeLog] = subj
		}
		{
			subj, err := srConf.FieldString(srFieldMetricSubject)
			if err != nil {
				return otlpInput{}, err
			}
			if subj == "" {
				subj = defaultSubject(SignalTypeMetric, o.encoding)
			}
			o.subject[SignalTypeMetric] = subj
		}
	}

	return o, nil
}

// maybeInitSchemaRegistry initializes Schema Registry by registering all signal
// type schemas and caching their IDs.
func (o *otlpInput) maybeInitSchemaRegistry(ctx context.Context) error {
	if o.srClient == nil {
		return nil // SR not configured, skip
	}

	o.schemaID = make(map[SignalType]int, 3)

	switch o.encoding {
	case EncodingProtobuf:
		commonRef, err := rpotel.RegisterCommonProtoSchema(ctx, o.srClient, o.commonSubject)
		if err != nil {
			return err
		}
		{
			ss, err := rpotel.RegisterTraceProtoSchema(ctx, o.srClient, o.subject[SignalTypeTrace], commonRef)
			if err != nil {
				return err
			}
			o.schemaID[SignalTypeTrace] = ss.ID
		}
		{
			ss, err := rpotel.RegisterLogProtoSchema(ctx, o.srClient, o.subject[SignalTypeLog], commonRef)
			if err != nil {
				return err
			}
			o.schemaID[SignalTypeLog] = ss.ID
		}
		{
			ss, err := rpotel.RegisterMetricProtoSchema(ctx, o.srClient, o.subject[SignalTypeMetric], commonRef)
			if err != nil {
				return err
			}
			o.schemaID[SignalTypeMetric] = ss.ID
		}
	case EncodingJSON:
		{
			ss, err := rpotel.RegisterTraceJSONSchema(ctx, o.srClient, o.subject[SignalTypeTrace])
			if err != nil {
				return err
			}
			o.schemaID[SignalTypeTrace] = ss.ID
		}
		{
			ss, err := rpotel.RegisterLogJSONSchema(ctx, o.srClient, o.subject[SignalTypeLog])
			if err != nil {
				return err
			}
			o.schemaID[SignalTypeLog] = ss.ID
		}
		{
			ss, err := rpotel.RegisterMetricJSONSchema(ctx, o.srClient, o.subject[SignalTypeMetric])
			if err != nil {
				return err
			}
			o.schemaID[SignalTypeMetric] = ss.ID
		}
	default:
		panic("unreachable")
	}

	for signalType, schemaID := range o.schemaID {
		o.log.Infof("Using Schema Registry schema ID %d for signal type %s", schemaID, signalType.String())
	}

	return nil
}

// maybeWaitForAccess blocks until the rate limiter grants access or the
// context/shutdown signals. If no rate limit is configured, it returns
// immediately. It must be called before calling [sendMessageBatch].
func (o *otlpInput) maybeWaitForAccess(ctx context.Context) {
	if o.rateLimit == "" {
		return
	}

	for {
		var (
			d   time.Duration
			err error
		)
		if rerr := o.mgr.AccessRateLimit(ctx, o.rateLimit, func(rl service.RateLimit) {
			d, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if err != nil {
			o.log.Errorf("Rate limit error: %v", err)
			d = time.Second
		}

		if d == 0 {
			return
		}

		// Wait for the duration or shutdown
		select {
		case <-ctx.Done():
			return
		case <-o.shutSig.SoftStopChan():
			return
		case <-time.After(d):
			return
		}
	}
}

// sendMessageBatch sends a pre-constructed message batch through the pipeline.
// The function blocks until either:
//
//   - The batch is successfully queued (returns ack channel)
//   - The context is canceled (returns ctx.Err())
//   - The input is shutting down (returns service.ErrNotConnected)
func (o *otlpInput) sendMessageBatch(ctx context.Context, batch service.MessageBatch) (chan error, error) {
	// Send batch through channel
	resCh := make(chan error, 1)
	select {
	case o.resCh <- asyncMessage{
		msg: batch,
		ackFn: func(_ context.Context, err error) error {
			select {
			case resCh <- err:
			default:
				o.log.Warnf("Acknowledgment channel full, dropping ack error: %v", err)
			}
			return nil
		},
	}:
		return resCh, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.shutSig.SoftStopChan():
		return nil, service.ErrNotConnected
	}
}

// ReadBatch reads a batch of messages.
func (o *otlpInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-o.shutSig.HasStoppedChan():
		return nil, nil, service.ErrEndOfInput
	case am := <-o.resCh:
		return am.msg, am.ackFn, nil
	}
}

// newMessageWithSignalType creates a new message from a protobuf object with
// the specified signal type metadata and encoding configured for this input.
func (o *otlpInput) newMessageWithSignalType(msg proto.Message, s SignalType) (*service.Message, error) {
	var (
		msgBytes []byte
		err      error
	)
	switch o.encoding {
	case EncodingProtobuf:
		msgBytes, err = proto.Marshal(msg)
	case EncodingJSON:
		marshaler := protojson.MarshalOptions{
			UseProtoNames:  true, // Align with our snake case preferences
			UseEnumNumbers: true, // Closer to the official OTEL JSON format
		}
		msgBytes, err = marshaler.Marshal(msg)
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", o.encoding)
	}
	if err != nil {
		return nil, err
	}

	// Add Schema Registry header if configured
	if schemaID, ok := o.schemaID[s]; ok {
		msgBytes, err = o.insertSchemaRegistryHeader(schemaID, msgBytes)
		if err != nil {
			return nil, fmt.Errorf("insert schema registry header: %w", err)
		}
	}

	svcMsg := service.NewMessage(msgBytes)
	svcMsg.MetaSet(MetadataKeySignalType, s.String())
	svcMsg.MetaSet(MetadataKeyEncoding, o.encoding.String())
	return svcMsg, nil
}

// insertSchemaRegistryHeader prepends the Confluent Schema Registry wire format
// header to the payload.
func (o *otlpInput) insertSchemaRegistryHeader(schemaID int, payload []byte) ([]byte, error) {
	var (
		header sr.ConfluentHeader
		index  []int
	)
	if o.encoding == EncodingProtobuf {
		index = []int{0} // top-level message for protobuf
	}
	h, err := header.AppendEncode(nil, schemaID, index)
	if err != nil {
		return payload, err
	}

	n := len(h)
	res := slices.Grow(payload, n)[:len(payload)+n]
	copy(res[n:], payload)
	copy(res[:n], h)
	return res, nil
}
