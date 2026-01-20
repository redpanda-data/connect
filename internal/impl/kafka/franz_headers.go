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

// kafkaHeaders is the metadata key under which the full list of Kafka headers
// is stored.
const kafkaHeaders = "__rpcn_kafka_headers"

// AddHeaders stores Kafka record headers in message metadata. Each header value
// is stored under its key. Empty values are stored as nil, other values
// as string. The full original list of headers is stored under the 
// special key "__rpcn_kafka_headers".
func AddHeaders(msg *service.Message, headers []kgo.RecordHeader) {
	if len(headers) == 0 {
		return
	}

	for _, h := range headers {
		if h.Value == nil {
			msg.MetaSetMut(h.Key, nil)
		} else if n := len(h.Value); n == 0 {
			msg.MetaSetMut(h.Key, "")
		} else {
			msg.MetaSetMut(h.Key, string(h.Value))
		}
	}
	msg.MetaSetMut(kafkaHeaders, headers)
}

// ExtractHeaders reconstructs Kafka record headers from message metadata.
// Returns nil if no headers are present. This is the inverse of [AddHeaders].
func ExtractHeaders(msg *service.Message) []kgo.RecordHeader {
	m, ok := msg.MetaGetMut(kafkaHeaders)
	if !ok {
		return nil
	}
	headers, ok := m.([]kgo.RecordHeader)
	if !ok {
		return nil
	}
	return headers
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

// SetHeaderValue sets the last header value matching the given key. If the key
// is not found, a new header is appended to the end of the list.
// The returned slice references the original header data and must not be
// modified.
func SetHeaderValue(headers []kgo.RecordHeader, key string, value []byte) []kgo.RecordHeader {
	for i := range headers {
		h := &headers[len(headers)-1-i]
		if h.Key == key {
			h.Value = value
			return headers
		}
	}
	headers = append(headers, kgo.RecordHeader{
		Key:   key,
		Value: value,
	})
	return headers
}
