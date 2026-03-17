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
	"google.golang.org/protobuf/proto"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// NewMessageWithSignalType is a test helper that creates a message with the given encoding.
func NewMessageWithSignalType(msg proto.Message, s SignalType, enc Encoding) (*service.Message, error) {
	// Create a temporary otlpInput with the specified encoding
	input := otlpInput{encoding: enc}
	return input.newMessageWithSignalType(msg, s)
}
