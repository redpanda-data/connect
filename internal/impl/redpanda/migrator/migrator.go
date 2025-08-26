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
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rmoFieldTopic                  = "topic"
	rmoFieldTopicReplicationFactor = "topic_replication_factor"
	rmoFieldSyncTopicACLs          = "sync_topic_acls"
	rmoFieldServerless             = "serverless"
)

func migratorInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("TODO").
		// Kafka fields
		Fields(kafka.FranzConnectionFields()...).
		Fields(kafka.FranzConsumerFields()...).
		Fields(kafka.FranzReaderOrderedConfigFields()...).
		LintRule(kafka.FranzConsumerFieldLintRules).
		// Schema registry fields
		Field(schemaRegistryField().Optional()).
		// Other fields
		Field(service.NewAutoRetryNacksToggleField())
}

func migratorOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("TODO").
		// Kafka fields
		Fields(kafka.FranzConnectionFields()...).
		Fields(kafka.FranzProducerFields()...).
		Fields(kafka.FranzWriterConfigFields()...).
		// Schema registry fields
		Field(schemaRegistryField(schemaRegistryMigratorFields()...).Optional()).
		// Topic fields
		Field(service.NewInterpolatedStringField(rmoFieldTopic).
			Description("The topic to write messages to, the source topic name is passed as kafka_topic metadata.").
			Optional()).
		Field(service.NewIntField(rmoFieldTopicReplicationFactor).
			Description("The replication factor for the created topics if different from the source cluster.").
			Optional()).
		// ACL fields
		Field(service.NewBoolField(rmoFieldSyncTopicACLs).
			Description("Whether to configure remote topic ACLs to match their corresponding upstream topics.").
			Default(false)).
		Field(service.NewBoolField(rmoFieldServerless).
			Description("Set this to `true` when using Serverless clusters in Redpanda Cloud as the destination cluster.").
			Default(false).
			Advanced())
}

type migratorResKey string

func migratorKey(label string) migratorResKey {
	if label == "" {
		label = "default"
	}
	return migratorResKey("migrator_" + label)
}

func newMigratorFrom(mgr *service.Resources) *Migrator {
	m, _ := mgr.GetOrSetGeneric(migratorKey(mgr.Label()), NewMigrator(mgr))
	return m.(*Migrator)
}

type migratorBatchInput struct {
	service.BatchInput
	m *Migrator
}

func (w migratorBatchInput) Connect(ctx context.Context) error {
	if err := w.BatchInput.Connect(ctx); err != nil {
		return err
	}
	return w.m.OnInputConnected(ctx, w.BatchInput.(*kafka.FranzReaderOrdered))
}

type migratorBatchOutput struct {
	service.BatchOutput
	m *Migrator
}

func (w migratorBatchOutput) Connect(ctx context.Context) error {
	if err := w.BatchOutput.Connect(ctx); err != nil {
		return err
	}
	return w.m.OnOutputConnected(ctx, w.BatchOutput.(franzWriter))
}

func init() {
	service.MustRegisterBatchInput("redpanda_migrator2", migratorInputConfig(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			m := newMigratorFrom(mgr)
			if err := m.initInputFromParsed(pConf, mgr); err != nil {
				return nil, err
			}

			fr, err := newFranzReaderOrdered(pConf, mgr)
			if err != nil {
				return nil, err
			}
			m.srcAdm = kadm.NewClient(fr.Client)

			return service.AutoRetryNacksBatchedToggled(pConf, migratorBatchInput{fr, m})
		})

	service.MustRegisterBatchOutput("redpanda_migrator2", migratorOutputConfig(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			m := newMigratorFrom(mgr)

			err = m.initOutputFromParsed(pConf, mgr)
			if err != nil {
				return
			}

			fw, err := newFranzWriter(pConf, mgr)
			if err != nil {
				return
			}
			fw.OnWrite = m.onWrite
			out = migratorBatchOutput{fw, m}

			// Force single in-flight batch message to ensure data ordering
			maxInFlight = 1

			return
		})
}

//------------------------------------------------------------------------------

// Migrator handles the migration of data between Kafka clusters with schema
// registry support.
type Migrator struct {
	topic topicMigrator
	sr    schemaRegistryMigrator
	log   *service.Logger

	plumbing uint8
	stopSig  *shutdown.Signaller

	mu     sync.RWMutex
	src    *kgo.Client
	srcAdm *kadm.Client
}

// NewMigrator creates a new Migrator instance with the provided logger.
func NewMigrator(mgr *service.Resources) *Migrator {
	log := mgr.Logger()
	return &Migrator{
		topic: topicMigrator{
			metrics:     newTopicMetrics(mgr.Metrics()),
			log:         log,
			knownTopics: make(map[string]string),
		},
		sr: schemaRegistryMigrator{
			metrics:      newSchemaRegistryMetrics(mgr.Metrics()),
			log:          log,
			knownSchemas: make(map[int]schemaInfo),
			compatSet:    make(map[string]struct{}),
		},
		log:     log,
		stopSig: shutdown.NewSignaller(),
	}
}

func (m *Migrator) initInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	var err error

	m.sr.src, err = schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}

	m.plumbing |= inputInitialized
	return nil
}

func (m *Migrator) initOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	var err error

	if err := m.topic.conf.initFromParsed(pConf); err != nil {
		return err
	}

	m.sr.dst, err = schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}
	if err := m.sr.conf.initFromParsed(pConf); err != nil {
		return err
	}

	m.plumbing |= outputInitialized
	return nil
}

func (m *Migrator) OnInputConnected(_ context.Context, fr *kafka.FranzReaderOrdered) error { //nolint:revive
	if err := m.validateInitialized(); err != nil {
		return err
	}

	m.mu.Lock()
	m.src = fr.Client
	m.srcAdm = kadm.NewClient(fr.Client)
	m.mu.Unlock()

	return nil
}

func (m *Migrator) OnOutputConnected(_ context.Context, fw franzWriter) error { //nolint:revive
	if err := m.validateInitialized(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()
		<-m.stopSig.SoftStopChan()
	}()

	// Syncing topics is deferred until the first message is received because
	// we use GetConsumeTopics, which is only available after the first message
	// is received.

	// Sync the schema registry
	if err := m.sr.Sync(ctx); err != nil {
		return err
	}
	go m.sr.SyncLoop(ctx)

	return nil
}

func (m *Migrator) validateInitialized() error {
	if m.plumbing&inputInitialized == 0 {
		return errors.New("input not initialized")
	}
	if m.plumbing&outputInitialized == 0 {
		return errors.New("output not initialized")
	}
	// If schema registry migration is disabled, allow client mismatch.
	if !m.sr.conf.Enabled {
		return nil
	}
	if m.sr.src != nil && m.sr.dst == nil || m.sr.dst != nil && m.sr.src == nil {
		return errors.New("schema registry mismatch: both input and output must be set")
	}
	return nil
}

func (m *Migrator) onWrite(
	ctx context.Context,
	dst *kgo.Client,
	records []*kgo.Record,
) error {
	m.mu.RLock()
	src := m.src
	srcAdm := m.srcAdm
	m.mu.RUnlock()

	dstAdm := kadm.NewClient(dst)

	if err := m.topic.SyncOnce(ctx, srcAdm, dstAdm, src.GetConsumeTopics); err != nil {
		return fmt.Errorf("sync topics: %w", err)
	}

	var (
		lastTopic       string
		lastDstTopic    string
		lastSchemaID    int
		lastDstSchemaID int
	)
	for _, record := range records {
		if record.Topic != lastTopic {
			topic, err := m.topic.CreateTopicIfNeeded(ctx, srcAdm, dstAdm, record.Topic)
			if err != nil {
				return err
			}
			lastTopic, lastDstTopic = record.Topic, topic
		}
		record.Topic = lastDstTopic

		// Update schema ID
		schemaID, err := parseSchemaID(record.Value)
		if err != nil {
			return fmt.Errorf("parse schema ID: %w", err)
		}
		if schemaID != 0 {
			if schemaID != lastSchemaID {
				id, err := m.sr.DestinationSchemaID(ctx, schemaID)
				if err != nil {
					return fmt.Errorf("resolve destination schema ID: %w", err)
				}
				lastSchemaID, lastDstSchemaID = schemaID, id
			}
			if err := updateSchemaID(record.Value, lastDstSchemaID); err != nil {
				return fmt.Errorf("update schema ID: %w", err)
			}
		}
	}

	return nil
}

func parseSchemaID(b []byte) (int, error) {
	if b == nil {
		return 0, nil
	}

	var ch sr.ConfluentHeader
	schemaID, _, err := ch.DecodeID(b)
	if err != nil && !errors.Is(err, sr.ErrBadHeader) {
		return 0, fmt.Errorf("decode schema ID: %w", err)
	}
	return schemaID, nil
}

func updateSchemaID(b []byte, schemaID int) error {
	var ch sr.ConfluentHeader
	return ch.UpdateID(b, uint32(schemaID))
}
