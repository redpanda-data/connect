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

package otlp_test

import (
	pb "github.com/redpanda-data/common-go/redpanda-otel-exporter/proto"
)

func attrMap(attrs []*pb.KeyValue) map[string]*pb.AnyValue {
	attrMap := make(map[string]*pb.AnyValue)
	for _, kv := range attrs {
		attrMap[kv.Key] = kv.Value
	}
	return attrMap
}

func attrGet(attrs []*pb.KeyValue, key string) *pb.AnyValue {
	for _, kv := range attrs {
		if kv.Key == key {
			return kv.Value
		}
	}
	return nil
}
