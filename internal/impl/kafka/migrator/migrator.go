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

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/confluent/sr"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
)

const (
	rmoFieldTopicReplicationFactor = "topic_replication_factor"
	rmoFieldMaxInFlight            = "max_in_flight"
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
		// Other fields
		Field(service.NewIntMapField(rmoFieldTopicReplicationFactor).Optional().Description("The replication factor for the created topics if different from the source cluster.")).
		Field(service.NewIntField(rmoFieldMaxInFlight).Description("The maximum number of batches to be sending in parallel at any given time.").Default(256))
}

type migratorResKey string

func migratorKey(label string) migratorResKey {
	if label == "" {
		label = "default"
	}
	return migratorResKey("migrator_" + label)
}

func migratorFrom(mgr *service.Resources) *Migrator {
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
	return w.m.OnConnect(ctx)
}

type migratorBatchOutput struct {
	service.BatchOutput
	m *Migrator
}

func (w migratorBatchOutput) Connect(ctx context.Context) error {
	if err := w.BatchOutput.Connect(ctx); err != nil {
		return err
	}
	return w.m.OnConnect(ctx)
}

func init() {
	service.MustRegisterBatchInput("redpanda_migrator_v2", migratorInputConfig(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			m := migratorFrom(mgr)
			if err := m.initInputFromParsed(pConf, mgr); err != nil {
				return nil, err
			}

			out, err := newFranzReaderOrdered(pConf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatchedToggled(pConf, migratorBatchInput{out, m})
		})
	service.MustRegisterBatchOutput("redpanda_migrator_v2", migratorOutputConfig(),
		func(pConf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			m := migratorFrom(mgr)

			err = m.initOutputFromParsed(pConf, mgr)
			if err != nil {
				return
			}

			fw, err := newFranzWriter(pConf, mgr)
			if err != nil {
				return
			}
			fw.OnWrite = m.OnWrite
			out = migratorBatchOutput{fw, m}

			maxInFlight, err = pConf.FieldInt(rmoFieldMaxInFlight)
			if err != nil {
				return
			}

			return
		})
}

//------------------------------------------------------------------------------

// Migrator handles the migration of data between Kafka clusters with schema registry support.
type Migrator struct {
	sri *sr.Client
	sro *sr.Client
	log *service.Logger

	plumbing uint8
}

// NewMigrator creates a new Migrator instance with the provided logger.
func NewMigrator(log *service.Logger) *Migrator {
	return &Migrator{
		log: log,
	}
}

func (m *Migrator) initInputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	sri, err := schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}
	m.sri = sri
	m.plumbing |= inputInitialized
	return nil
}

func (m *Migrator) initOutputFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) error {
	sro, err := schemaRegistryClientFromParsed(pConf, mgr)
	if err != nil {
		return err
	}
	m.sro = sro
	m.plumbing |= outputInitialized
	return nil
}

func (m *Migrator) OnConnect(ctx context.Context) error { //nolint:revive
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

func (m *Migrator) OnWrite(ctx context.Context, client *kgo.Client, records []*kgo.Record) error { //nolint:revive
	m.log.Infof("Processing %d records", len(records))
	return nil
}
