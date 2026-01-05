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
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// SignalType represents the type of OpenTelemetry signal (trace, log, or metric).
type SignalType string

const (
	// SignalTypeTrace represents the trace signal type
	SignalTypeTrace SignalType = "trace"
	// SignalTypeLog represents the log signal type
	SignalTypeLog SignalType = "log"
	// SignalTypeMetric represents the metric signal type
	SignalTypeMetric SignalType = "metric"
)

// MetadataKeySignalType is the metadata key used to store the signal type.
const MetadataKeySignalType = "signalType"

// String returns the string representation of the SignalType.
func (s SignalType) String() string {
	return string(s)
}

// newMessageWithSignalType creates a new message from a protobuf object with
// the specified signal type metadata.
func newMessageWithSignalType(msg proto.Message, s SignalType) (*service.Message, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	svcMsg := service.NewMessage(msgBytes)
	svcMsg.MetaSet(MetadataKeySignalType, s.String())
	return svcMsg, nil
}
