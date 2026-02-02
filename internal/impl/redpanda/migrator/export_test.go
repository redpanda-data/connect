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
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var (
	TopicDetailsWithClient = topicDetailsWithClient
	DescribeACLs           = describeACLs
	SchemaStringEquals     = schemaStringEquals
	EncodeOffsetHeader     = encodeOffsetHeader
)

func ReadRecordTimestamp(
	ctx context.Context,
	client *kgo.Client,
	topic string,
	topicID kadm.TopicID,
	partition int32,
	offset int64,
	fetchTimeout time.Duration,
) (time.Time, error) {
	r, err := readRecordAtOffset(ctx, client, topic, topicID, partition, offset, fetchTimeout)
	if err != nil {
		return time.Time{}, err
	}
	return r.Timestamp, nil
}

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
		conf:   conf,
		src:    src,
		srcURL: "src",
		dst:    dst,
		dstURL: "dst",
		log: service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))),
		knownSubjects: make(map[schemaSubjectVersion]struct{}),
		knownSchemas:  make(map[int]schemaInfo),
	}
}

func (m *schemaRegistryMigrator) DfsSubjectSchemasFunc(
	ctx context.Context,
	client *sr.Client,
	root sr.SubjectSchema,
	filter func(subject string, version int) bool,
	cb func(sr.SubjectSchema) error,
) error {
	return m.dfsSubjectSchemasFunc(ctx, client, root, filter, cb)
}

func NewGroupsMigratorForTesting(
	t *testing.T,
	conf GroupsMigratorConfig,
	src, dst *kgo.Client,
	srcAdm, dstAdm *kadm.Client,
) *groupsMigrator {
	var buf bytes.Buffer
	t.Cleanup(func() {
		t.Log(buf.String())
	})
	return &groupsMigrator{
		conf:         conf,
		offsetHeader: DefaultOffsetHeader,
		src:          src,
		srcAdm:       srcAdm,
		dst:          dst,
		dstAdm:       dstAdm,
		log: service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))),
		topicIDs:        make(map[string]kadm.TopicID),
		dstTopicIDs:     make(map[string]kadm.TopicID),
		commitedOffsets: make(map[string]map[string]map[int32][2]int64),
	}
}
