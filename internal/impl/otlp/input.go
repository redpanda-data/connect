// Copyright 2026 Redpanda Data, Inc.
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

package otlp

import (
	"context"
	"encoding/binary"
	"slices"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/schemaregistry"
)

type asyncMessage struct {
	msg   service.MessageBatch
	ackFn service.AckFunc
}

type otlpInput struct {
	log       *service.Logger
	mgr       *service.Resources
	rateLimit string
	resCh     chan asyncMessage
	shutSig   *shutdown.Signaller

	srClient  *sr.Client
	schemaIDs map[SignalType]int
}

func newOTLPInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources, rateLimit string) (otlpInput, error) {
	client, err := schemaregistry.ClientFromParsedOptional(pConf, schemaRegistryField, mgr)
	if err != nil {
		return otlpInput{}, err
	}

	return otlpInput{
		log:       mgr.Logger(),
		mgr:       mgr,
		rateLimit: rateLimit,
		resCh:     make(chan asyncMessage),
		shutSig:   shutdown.NewSignaller(),

		srClient: client,
	}, nil
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

// initSchemaRegistry initializes Schema Registry by registering all three
// signal type schemas and caching their IDs. Called once during Connect().
func (o *otlpInput) initSchemaRegistry(ctx context.Context) error {
	if o.srClient == nil {
		return nil // SR not configured, skip
	}

	schemaIDs, err := registerSchemas(ctx, o.srClient)
	if err != nil {
		return err
	}
	o.schemaIDs = schemaIDs

	// Log registered schemas
	for signalType, schemaID := range schemaIDs {
		o.log.Infof("Using Schema Registry schema ID %d for signal type %s", schemaID, signalType.String())
	}

	return nil
}

// newMessageWithSignalType creates a new message from a protobuf object with
// the specified signal type metadata and optional Schema Registry header.
func (o *otlpInput) newMessageWithSignalType(msg proto.Message, s SignalType) (*service.Message, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	// Add Schema Registry header if configured
	if schemaID, ok := o.schemaIDs[s]; ok {
		msgBytes = insertSchemaRegistryHeader(schemaID, msgBytes)
	}

	svcMsg := service.NewMessage(msgBytes)
	svcMsg.MetaSet(MetadataKeySignalType, s.String())
	return svcMsg, nil
}

func insertSchemaRegistryHeader(schemaID int, payload []byte) []byte {
	result := slices.Grow(payload, 5)[:len(payload)+5]
	copy(result[5:], payload)
	result[0] = 0x00 // Confluent magic byte
	binary.BigEndian.PutUint32(result[1:5], uint32(schemaID))
	return result
}
