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

package telemetry

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// benthosLogger adapts a benthos *service.Logger to the telemetry.Logger
// interface expected by the shared common-go/telemetry client.
type benthosLogger struct {
	l *service.Logger
}

func (b benthosLogger) Debug(msg string, kv ...any) {
	logger := b.l
	for i := 0; i+1 < len(kv); i += 2 {
		logger = logger.With(fmt.Sprint(kv[i]), kv[i+1])
	}
	logger.Debug(msg)
}
