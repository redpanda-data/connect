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

import "github.com/redpanda-data/benthos/v4/public/service"

type logWrapper struct {
	l *service.Logger
}

func (l *logWrapper) Errorf(format string, v ...interface{}) {
	l.l.With("component", "resty").Debugf(format, v...)
}

func (*logWrapper) Warnf(string, ...interface{}) {
	// Ignore
}

func (*logWrapper) Debugf(string, ...interface{}) {
	// Ignore
}
