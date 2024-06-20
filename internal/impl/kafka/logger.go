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

// KGoLogger wraps a service.Logger with an implementation that works within
// the kgo library.
type KGoLogger struct {
	L *service.Logger
}

// Level returns the logger level.
func (k *KGoLogger) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

// Log calls the underlying logger implementation using the appropriate log level.
func (k *KGoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	tmpL := k.L
	if len(keyvals) > 0 {
		tmpL = k.L.With(keyvals...)
	}

	switch level {
	case kgo.LogLevelError:
		tmpL.Error(msg)
	case kgo.LogLevelWarn:
		tmpL.Warn(msg)
	case kgo.LogLevelInfo:
		tmpL.Debug(msg)
	case kgo.LogLevelDebug:
		tmpL.Trace(msg)
	}
}
