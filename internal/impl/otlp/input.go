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
	"time"

	"github.com/Jeffail/shutdown"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
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
}

func newOTLPInput(mgr *service.Resources, enc Encoding, rateLimit string) otlpInput {
	return otlpInput{
		log:       mgr.Logger(),
		mgr:       mgr,
		encoding:  enc,
		rateLimit: rateLimit,
		resCh:     make(chan asyncMessage),
		shutSig:   shutdown.NewSignaller(),
	}
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
		msgBytes, err = protojson.Marshal(msg)
	default:
		return nil, fmt.Errorf("unsupported encoding: %s", o.encoding)
	}
	if err != nil {
		return nil, err
	}

	svcMsg := service.NewMessage(msgBytes)
	svcMsg.MetaSet(MetadataKeySignalType, s.String())
	svcMsg.MetaSet(MetadataKeyEncoding, o.encoding.String())
	return svcMsg, nil
}
