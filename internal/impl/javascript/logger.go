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

package javascript

import "github.com/redpanda-data/benthos/v4/public/service"

// Logger wraps the service.Logger so that we can define the below methods.
type Logger struct {
	l *service.Logger
}

// Log will be used for "console.log()" in JS
func (l *Logger) Log(message string) {
	l.l.Info(message)
}

// Warn will be used for "console.warn()" in JS
func (l *Logger) Warn(message string) {
	l.l.Warn(message)
}

// Error will be used for "console.error()" in JS
func (l *Logger) Error(message string) {
	l.l.Error(message)
}
