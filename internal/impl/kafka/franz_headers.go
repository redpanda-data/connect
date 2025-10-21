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

package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// AddHeaders stores Kafka record headers in message metadata. Each header value
// is stored under its key. Empty values are stored as nil, single-byte values
// as rune, multibyte values as string. The ordered list of keys is stored under
// "kafka_headers" for reconstruction.
func AddHeaders(msg *service.Message, headers []kgo.RecordHeader) {
	if len(headers) == 0 {
		return
	}

	keys := make([]string, len(headers))
	for i, h := range headers {
		if h.Value == nil {
			msg.MetaSetMut(h.Key, nil)
		} else if n := len(h.Value); n == 0 {
			msg.MetaSetMut(h.Key, "")
		} else if n == 1 {
			msg.MetaSetMut(h.Key, rune(h.Value[0]))
		} else {
			msg.MetaSetMut(h.Key, string(h.Value))
		}
		keys[i] = h.Key
	}
	msg.MetaSetMut("kafka_headers", keys)
}

// AddHeadersRaw stores the complete Kafka record headers slice in message
// metadata under "kafka_headers_raw". This preserves the original header
// structure for efficient retrieval without conversion. Used by [ExtractHeaders]
// as the primary reconstruction method.
func AddHeadersRaw(msg *service.Message, headers []kgo.RecordHeader) {
	msg.MetaSetMut("kafka_headers_raw", headers)
}

// ExtractHeaders reconstructs Kafka record headers from message metadata.
// Returns nil if no headers are present. This is the inverse of [AddHeaders].
func ExtractHeaders(msg *service.Message) []kgo.RecordHeader {
	headers := extractRawHeaders(msg)
	if headers != nil {
		return headers
	}

	keys := extractKeys(msg)
	if len(keys) == 0 {
		return nil
	}

	headers = make([]kgo.RecordHeader, 0, len(keys))
	for _, k := range extractKeys(msg) {
		v, ok := msg.MetaGetMut(k)
		if !ok {
			continue
		}
		var vb []byte
		switch val := v.(type) {
		case nil:
			vb = nil
		case []byte:
			vb = val
		case rune:
			vb = []byte{byte(val)}
		case string:
			vb = []byte(val)
		default:
			continue
		}
		headers = append(headers, kgo.RecordHeader{
			Key:   k,
			Value: vb,
		})
	}

	return headers
}

func extractRawHeaders(msg *service.Message) []kgo.RecordHeader {
	headers, ok := msg.MetaGetMut("kafka_headers_raw")
	if !ok {
		return nil
	}
	raw, ok := headers.([]kgo.RecordHeader)
	if !ok {
		return nil
	}
	return raw
}

func extractKeys(msg *service.Message) []string {
	headers, ok := msg.MetaGetMut("kafka_headers")
	if !ok {
		return nil
	}
	keys, ok := headers.([]string)
	if !ok {
		return nil
	}
	return keys
}

// GetHeaderValue retrieves the last header value matching the given key.
// Returns nil if the key is not found. The returned slice references the
// original header data and must not be modified.
func GetHeaderValue(headers []kgo.RecordHeader, key string) ([]byte, bool) {
	for i := range headers {
		h := &headers[len(headers)-1-i]
		if h.Key == key {
			return h.Value, true
		}
	}
	return nil, false
}
