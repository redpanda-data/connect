// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	TopicDetailsWithClient = topicDetailsWithClient
	DescribeACLs           = describeACLs
	SchemaStringEquals     = schemaStringEquals
)

func NewTopicMigratorForTesting(t *testing.T, conf TopicMigratorConfig) *topicMigrator {
	var buf bytes.Buffer
	t.Cleanup(func() {
		t.Log(buf.String())
	})
	return &topicMigrator{
		conf: conf,
		log: service.NewLoggerFromSlog(
			slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))),
		knownTopics: make(map[string]TopicMapping),
	}
}

func NewSchemaRegistryMigratorForTesting(t *testing.T, conf SchemaRegistryMigratorConfig, src, dst *sr.Client) *schemaRegistryMigrator {
	var buf bytes.Buffer
	t.Cleanup(func() {
		t.Log(buf.String())
	})
	return &schemaRegistryMigrator{
		conf: conf,
		src:  src,
		dst:  dst,
		log: service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))),
		knownSchemas: make(map[int]schemaInfo),
		compatSet:    make(map[string]struct{}),
	}
}
