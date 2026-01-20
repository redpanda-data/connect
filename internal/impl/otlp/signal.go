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

// MetadataKeySignalType is the metadata key used to store the signal type.
const (
	MetadataKeySignalType = "signal_type"
	MetadataKeyEncoding   = "encoding"
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

// String returns the string representation of the SignalType.
func (s SignalType) String() string {
	return string(s)
}

// Encoding represents the message encoding format.
type Encoding string

const (
	// EncodingProtobuf represents protobuf binary encoding
	EncodingProtobuf Encoding = "protobuf"
	// EncodingJSON represents JSON encoding
	EncodingJSON Encoding = "json"
)

// String returns the string representation of the Encoding.
func (e Encoding) String() string {
	return string(e)
}
