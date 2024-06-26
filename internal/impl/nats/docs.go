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

package nats

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	kvFieldBucket = "bucket"
)

const (
	tracingVersion = "4.23.0"
)

func connectionNameDescription() string {
	return `== Connection name

When monitoring and managing a production NATS system, it is often useful to
know which connection a message was send/received from. This can be achieved by
setting the connection name option when creating a NATS connection.

Redpanda Connect will automatically set the connection name based off the label of the given
NATS component, so that monitoring tools between NATS and Redpanda Connect can stay in sync.
`
}

func inputTracingDocs() *service.ConfigField {
	return service.NewExtractTracingSpanMappingField().Version(tracingVersion)
}
func outputTracingDocs() *service.ConfigField {
	return service.NewInjectTracingSpanMappingField().Version(tracingVersion)
}
func kvDocs(extraFields ...*service.ConfigField) []*service.ConfigField {
	// TODO: Use `slices.Concat()` after switching to Go 1.22
	fields := append(
		connectionHeadFields(),
		[]*service.ConfigField{
			service.NewStringField(kvFieldBucket).
				Description("The name of the KV bucket.").Example("my_kv_bucket"),
		}...,
	)
	fields = append(fields, extraFields...)
	fields = append(fields, connectionTailFields()...)

	return fields
}
