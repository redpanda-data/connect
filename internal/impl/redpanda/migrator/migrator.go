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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rmoFieldTopic                  = "topic"
	rmoFieldTopicReplicationFactor = "topic_replication_factor"
	rmoFieldSyncTopicACLs          = "sync_topic_acls"
	rmoFieldMaxInFlight            = "max_in_flight"
	rmoFieldIsServerless           = "is_serverless"
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
		Field(schemaRegistryField().Optional()).
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
		// Other fields
		Field(service.NewIntField(rmoFieldMaxInFlight).
			Description("The maximum number of batches to be sending in parallel at any given time.").
			Default(256)).
		Field(service.NewBoolField(rmoFieldIsServerless).
			Description("Set this to `true` when using Serverless clusters in Redpanda Cloud.").
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
	m, _ := mgr.GetOrSetGeneric(migratorKey(mgr.Label()), NewMigrator(mgr.Logger()))
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
	service.MustRegisterBatchInput("redpanda_migrator_v2", migratorInputConfig(),
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

	service.MustRegisterBatchOutput("redpanda_migrator_v2", migratorOutputConfig(),
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

			maxInFlight, err = pConf.FieldInt(rmoFieldMaxInFlight)
			if err != nil {
				return
			}

			return
		})
}

//------------------------------------------------------------------------------

// Migrator handles the migration of data between Kafka clusters with schema
// registry support.
type Migrator struct {
	topic topicMigrator
	log   *service.Logger

	sri *sr.Client
	sro *sr.Client

	plumbing uint8

	mu     sync.RWMutex
	src    *kgo.Client
	srcAdm *kadm.Client
}

// NewMigrator creates a new Migrator instance with the provided logger.
func NewMigrator(log *service.Logger) *Migrator {
	return &Migrator{
		topic: topicMigrator{
			log:         log,
			knownTopics: make(map[string]string),
		},
		log: log,
	}
}

func (m *Migrator) initInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	var err error

	m.sri, err = schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}

	m.plumbing |= inputInitialized
	return nil
}

func (m *Migrator) initOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	var err error

	m.sro, err = schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}

	if err := m.topic.initFromParsed(pConf); err != nil {
		return err
	}

	m.plumbing |= outputInitialized
	return nil
}

func (m *Migrator) OnInputConnected(ctx context.Context, fr *kafka.FranzReaderOrdered) error { //nolint:revive
	if err := m.validateInitialized(); err != nil {
		return err
	}

	m.mu.Lock()
	m.src = fr.Client
	m.srcAdm = kadm.NewClient(fr.Client)
	m.mu.Unlock()

	return nil
}

func (m *Migrator) OnOutputConnected(ctx context.Context, fw franzWriter) error { //nolint:revive
	if err := m.validateInitialized(); err != nil {
		return err
	}

	return nil
}

func (m *Migrator) validateInitialized() error {
	if m.plumbing&inputInitialized == 0 {
		return errors.New("input not initialized")
	}
	if m.plumbing&outputInitialized == 0 {
		return errors.New("output not initialized")
	}
	if m.sri != nil && m.sro == nil || m.sro != nil && m.sri == nil {
		return errors.New("schema registry mismatch both input and output must be set")
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

	if err := m.topic.InitKnownTopics(ctx, srcAdm, dstAdm, src.GetConsumeTopics); err != nil {
		return fmt.Errorf("init known topics: %w", err)
	}

	var (
		lastTopic    string
		lastDstTopic string
	)
	for _, record := range records {
		// Update topic
		if record.Topic != lastTopic {
			topic, err := m.topic.CreateTopicIfNeeded(ctx, srcAdm, dstAdm, record.Topic)
			if err != nil {
				return err
			}
			lastTopic, lastDstTopic = record.Topic, topic
		}
		record.Topic = lastDstTopic
	}

	return nil
}
