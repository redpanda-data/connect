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

package pulsar

import (
	plog "github.com/apache/pulsar-client-go/pulsar/log"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// DefaultLogger returns a logger that wraps Benthos Modular logger.
func createDefaultLogger(l *service.Logger) plog.Logger {
	return defaultLogger{
		backend: l,
	}
}

type defaultLogger struct {
	backend *service.Logger
}

func (l defaultLogger) SubLogger(plog.Fields) plog.Logger {
	return l
}

func (l defaultLogger) WithFields(plog.Fields) plog.Entry {
	return l
}

func (l defaultLogger) WithField(string, any) plog.Entry {
	return l
}

func (l defaultLogger) WithError(error) plog.Entry {
	return l
}

func (l defaultLogger) Debug(args ...any) {
	l.backend.Debugf("%v", args)
}

func (l defaultLogger) Info(args ...any) {
	l.backend.Infof("%v", args)
}

func (l defaultLogger) Warn(args ...any) {
	l.backend.Warnf("%v", args)
}

func (l defaultLogger) Error(args ...any) {
	l.backend.Errorf("%v", args)
}

func (l defaultLogger) Debugf(format string, args ...any) {
	l.backend.Debugf(format, args)
}

func (l defaultLogger) Infof(format string, args ...any) {
	l.backend.Infof(format, args)
}

func (l defaultLogger) Warnf(format string, args ...any) {
	l.backend.Warnf(format, args)
}

func (l defaultLogger) Errorf(format string, args ...any) {
	l.backend.Errorf(format, args)
}

// NoopLogger returns a logger that does nothing.
func NoopLogger() plog.Logger {
	return noopLogger{}
}

type noopLogger struct{}

func (n noopLogger) SubLogger(plog.Fields) plog.Logger {
	return n
}

func (n noopLogger) WithFields(plog.Fields) plog.Entry {
	return n
}

func (n noopLogger) WithField(string, any) plog.Entry {
	return n
}

func (n noopLogger) WithError(error) plog.Entry {
	return n
}

func (noopLogger) Debug(...any) {}
func (noopLogger) Info(...any)  {}
func (noopLogger) Warn(...any)  {}
func (noopLogger) Error(...any) {}

func (noopLogger) Debugf(string, ...any) {}
func (noopLogger) Infof(string, ...any)  {}
func (noopLogger) Warnf(string, ...any)  {}
func (noopLogger) Errorf(string, ...any) {}
