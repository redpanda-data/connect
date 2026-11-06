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

package tracing

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// FlushIntervalField returns the flush_interval field of a tracer that exports
// spans through an OpenTelemetry batch span processor.
func FlushIntervalField(name string) *service.ConfigField {
	return service.NewDurationField(name).
		Description("The maximum time to wait before exporting a batch of tracing spans. When unset, the `OTEL_BSP_SCHEDULE_DELAY` environment variable is used if set, otherwise the OpenTelemetry SDK default of `5s`.").
		Optional()
}
