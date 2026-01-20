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
	"errors"
	"fmt"

	"github.com/Jeffail/shutdown"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type otlpOutput struct {
	log     *service.Logger
	mgr     *service.Resources
	shutSig *shutdown.Signaller
}

func newOTLPOutput(mgr *service.Resources) otlpOutput {
	return otlpOutput{
		log:     mgr.Logger(),
		mgr:     mgr,
		shutSig: shutdown.NewSignaller(),
	}
}

// detectSignalType determines the signal type from the first message in the
// batch. Assumes all messages in the batch have the same signal type.
func detectSignalType(batch service.MessageBatch) (SignalType, error) {
	if len(batch) == 0 {
		return "", errors.New("empty batch")
	}

	signalType, exists := batch[0].MetaGet(MetadataKeySignalType)
	if !exists {
		return "", fmt.Errorf("missing %s metadata on message", MetadataKeySignalType)
	}

	return SignalType(signalType), nil
}

// unmarshalBatch converts a batch of messages into a slice of protobuf messages.
// T must be a protobuf message type (pb.Span, pb.LogRecord, or pb.Metric).
// P must be a pointer to T that implements proto.Message.
//
// Automatically detects encoding by trying JSON first, then falling back to
// protobuf.
func unmarshalBatch[T any, P interface {
	*T
	proto.Message
}](batch service.MessageBatch, typeName string) ([]T, error) {
	results := make([]T, len(batch))

	for i, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return nil, fmt.Errorf("message %d: failed to get bytes: %w", i, err)
		}

		ptr := P(&results[i])
		jsonErr := protojson.Unmarshal(msgBytes, ptr)
		if jsonErr == nil {
			continue
		}
		if pbErr := proto.Unmarshal(msgBytes, ptr); pbErr != nil {
			return nil, fmt.Errorf("message %d: failed to unmarshal %s: %w", i, typeName, errors.Join(jsonErr, pbErr))
		}
	}

	return results, nil
}
