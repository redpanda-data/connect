// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// SharedGlobalRedpandaClientKey points to a generic resource for obtaining the
// global redpanda handle.
const SharedGlobalRedpandaClientKey = "__redpanda_global"

const (
	grwFieldPipelineID  = "pipeline_id"
	grwFieldLogsTopic   = "logs_topic"
	grwFieldLogsLevel   = "logs_level"
	grwFieldStatusTopic = "status_topic"

	// Deprecated fields
	grwFieldRackID = "rack_id"

	statusTickerDuration = time.Second * 30
	topicMetaKey         = "__connect_topic"
	keyMetaKey           = "__connect_key"
)

// GlobalRedpandaFields returns the set of config fields found within the global `redpanda` config section.
func GlobalRedpandaFields() []*service.ConfigField {
	return slices.Concat(
		kafka.FranzConnectionFields(),
		[]*service.ConfigField{
			service.NewStringField(grwFieldPipelineID).
				Description("An optional identifier for the pipeline, this will be present in logs and status updates sent to topics.").
				Default(""),
			service.NewStringField(grwFieldLogsTopic).
				Description("A topic to send process logs to.").
				Default("").
				Example("__redpanda.connect.logs"),
			service.NewStringEnumField(grwFieldLogsLevel, "debug", "info", "warn", "error").
				Default("info"),
			service.NewStringField(grwFieldStatusTopic).
				Description("A topic to send status updates to.").
				Default("").
				Example("__redpanda.connect.status"),

			// Deprecated
			service.NewStringField(grwFieldRackID).Deprecated(),
		},
		kafka.FranzProducerFields(),
	)
}

// GlobalRedpandaManager provides a single place to configure Redpanda config fields
type GlobalRedpandaManager struct {
	id string

	fallbackLogger *service.Logger
	o              *service.OwnedOutput
	oCustom        *service.OwnedOutput // Only used if the logger is a custom broker config

	// Logger
	topicLogger   *topicLogger
	statusEmitter *statusEmitter
}

// NewGlobalRedpandaManager constructs a global redpanda connection manager.
func NewGlobalRedpandaManager(id string) *GlobalRedpandaManager {
	t := &GlobalRedpandaManager{
		id:            id,
		topicLogger:   newTopicLogger(id),
		statusEmitter: newStatusEmitter(id),
	}
	return t
}

// SetFallbackLogger configures a fallback logger.
func (l *GlobalRedpandaManager) SetFallbackLogger(fLogger *service.Logger) {
	l.fallbackLogger = fLogger
}

// TriggerEventStopped attempts to emit a status event (when initialised) that
// indicates a stream has stopped.
func (l *GlobalRedpandaManager) TriggerEventStopped(err error) {
	l.statusEmitter.TriggerEventStopped(err)
}

// SetStreamSummary configures a stream summary to use for broadcasting
// connectivity statuses.
func (l *GlobalRedpandaManager) SetStreamSummary(summary *service.RunningStreamSummary) {
	l.statusEmitter.SetStreamSummary(summary)
}

// SlogHandler returns a slog.Handler that is suitable for writing logs directly
// into a redpanda topic.
func (l *GlobalRedpandaManager) SlogHandler() slog.Handler {
	return l.topicLogger
}

// InitWithCustomDetails initialises the underlying logs and status writers with
// custom broker configuration that will only be used for the topic logger and
// status emitter, not for the shared redpanda common components.
//
// This should always be called before any configuration based initialisation.
func (l *GlobalRedpandaManager) InitWithCustomDetails(pipelineID, logsTopic, statusTopic string, connDetails *kafka.FranzConnectionDetails) error {
	connDetails.Logger = l.fallbackLogger

	w, err := newTopicLoggerWriterFromExplicit(l.fallbackLogger, connDetails)
	if err != nil {
		return err
	}
	if w == nil {
		return nil
	}

	// TODO: Enterprise check here?

	res := service.MockResources(service.MockResourcesOptUseLogger(l.fallbackLogger))
	tmpO, err := wrapWriter(res, w)
	if err != nil {
		l.fallbackLogger.With("error", err.Error()).Warn("failed to initialise topic logs connection")
		return err
	}

	l.oCustom = tmpO
	l.topicLogger.InitWithOutput(pipelineID, logsTopic, slog.LevelInfo, l.oCustom)
	l.statusEmitter.InitWithOutput(pipelineID, statusTopic, l.fallbackLogger, l.oCustom)

	return nil
}

// InitFromParsedConfig initialises the shared broker connection for redpanda
// common components, and also the underlying logs and status writers, unless a
// custom initialisation has already trigger those.
func (l *GlobalRedpandaManager) InitFromParsedConfig(pConf *service.ParsedConfig) error {
	w, err := newTopicLoggerWriterFromConfig(pConf, l.fallbackLogger)
	if err != nil {
		return err
	}
	if w == nil {
		return nil
	}

	var pipelineID string
	if pipelineID, err = pConf.FieldString(grwFieldPipelineID); err != nil {
		return err
	}

	var logsTopic, logsLevelStr, statusTopic string
	if logsTopic, err = pConf.FieldString(grwFieldLogsTopic); err != nil {
		return err
	}

	if logsLevelStr, err = pConf.FieldString(grwFieldLogsLevel); err != nil {
		return err
	}

	var logsLevel slog.Level
	switch strings.ToLower(logsLevelStr) {
	case "debug":
		logsLevel = slog.LevelDebug
	case "info":
		logsLevel = slog.LevelInfo
	case "warn":
		logsLevel = slog.LevelWarn
	case "error":
		logsLevel = slog.LevelError
	default:
		return fmt.Errorf("log level not recognized: %v", logsLevelStr)
	}

	if statusTopic, err = pConf.FieldString(grwFieldStatusTopic); err != nil {
		return err
	}

	if logsTopic != "" || statusTopic != "" {
		if err := license.CheckRunningEnterprise(pConf.Resources()); err != nil {
			return fmt.Errorf("unable to send logs or status events to redpanda: %w", err)
		}
	}

	res := service.MockResources(service.MockResourcesOptUseLogger(l.fallbackLogger))
	tmpO, err := wrapWriter(res, w)
	if err != nil {
		l.fallbackLogger.With("error", err.Error()).Warn("failed to initialise topic logs connection")
		return err
	}

	l.o = tmpO

	// All code paths from here have established an initialised status emitter,
	// so we ensure we trigger a config parse signal at the end.
	defer l.statusEmitter.TriggerEventConfigParsed()

	if l.oCustom != nil {
		// We've already initialised our logger and status emitter.
		return nil
	}

	l.topicLogger.InitWithOutput(pipelineID, logsTopic, logsLevel, l.o)
	l.statusEmitter.InitWithOutput(pipelineID, statusTopic, l.fallbackLogger, l.o)

	return nil
}

func wrapWriter(res *service.Resources, w service.BatchOutput) (*service.OwnedOutput, error) {
	tmpO, err := res.ManagedBatchOutput("redpanda_logger", 24, w)
	if err != nil {
		return nil, err
	}

	batchPol, err := (service.BatchPolicy{
		Count:  50,
		Period: "1s",
	}).NewBatcher(service.MockResources())
	if err != nil {
		return nil, err
	}

	tmpO = tmpO.BatchedWith(batchPol)
	if err := tmpO.PrimeBuffered(100); err != nil {
		return nil, err
	}

	return tmpO, nil
}

// Close the underlying connections of this manager.
func (l *GlobalRedpandaManager) Close(ctx context.Context) error {
	if l.o == nil && l.oCustom == nil {
		return nil
	}

	if err := l.topicLogger.Close(ctx); err != nil {
		return err
	}
	if err := l.statusEmitter.Close(ctx); err != nil {
		return err
	}

	o := l.o
	if o != nil {
		l.o = nil
		if err := o.Close(ctx); err != nil {
			return err
		}
	}

	o = l.oCustom
	if o != nil {
		l.oCustom = nil
		if err := o.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------

type franzTopicLoggerWriter struct {
	connDetails *kafka.FranzConnectionDetails
	clientOpts  []kgo.Opt
	client      *kgo.Client

	log *service.Logger
	mgr *service.Resources
}

func newTopicLoggerWriterFromExplicit(log *service.Logger, connDetails *kafka.FranzConnectionDetails) (*franzTopicLoggerWriter, error) {
	if len(connDetails.SeedBrokers) == 0 {
		return nil, nil
	}

	f := franzTopicLoggerWriter{
		log:         log,
		connDetails: connDetails,
	}

	f.clientOpts = f.connDetails.FranzOpts()

	// All other options (producer, etc) is currently set to the defaults.
	f.clientOpts = append(f.clientOpts, kgo.AllowAutoTopicCreation()) // TODO: Configure this?

	return &f, nil
}

func newTopicLoggerWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*franzTopicLoggerWriter, error) {
	f := franzTopicLoggerWriter{
		log: log,
		mgr: conf.Resources(),
	}

	if testList, _ := conf.FieldStringList("seed_brokers"); len(testList) == 0 {
		return nil, nil
	}

	var err error
	if f.connDetails, err = kafka.FranzConnectionDetailsFromConfig(conf, log); err != nil {
		return nil, err
	}
	f.clientOpts = f.connDetails.FranzOpts()

	var tmpOpts []kgo.Opt
	if tmpOpts, err = kafka.FranzProducerOptsFromConfig(conf); err != nil {
		return nil, err
	}
	f.clientOpts = append(f.clientOpts, tmpOpts...)

	return &f, nil
}

//------------------------------------------------------------------------------

func (f *franzTopicLoggerWriter) Connect(context.Context) error {
	if f.client != nil {
		return nil
	}

	cl, err := kafka.NewFranzClient(f.clientOpts...)
	if err != nil {
		return err
	}

	if f.mgr != nil {
		if err := kafka.FranzSharedClientSet(SharedGlobalRedpandaClientKey, &kafka.FranzSharedClientInfo{
			Client:      cl,
			ConnDetails: f.connDetails,
		}, f.mgr); err != nil {
			return fmt.Errorf("failed to store global redpanda client: %w", err)
		}
	}

	f.client = cl
	return nil
}

func (f *franzTopicLoggerWriter) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	if f.client == nil {
		return service.ErrNotConnected
	}

	records := make([]*kgo.Record, 0, len(b))
	for _, msg := range b {
		topic, _ := msg.MetaGet(topicMetaKey)
		if topic == "" {
			continue
		}
		var key []byte
		if keyStr, _ := msg.MetaGet(keyMetaKey); keyStr != "" {
			key = []byte(keyStr)
		}
		record := &kgo.Record{
			Key:   key,
			Topic: topic,
		}
		if record.Value, err = msg.AsBytes(); err != nil {
			return
		}
		records = append(records, record)
	}

	// TODO: This is very cool and allows us to easily return granular errors,
	// so we should honor travis by doing it.
	err = f.client.ProduceSync(ctx, records...).FirstErr()
	return
}

func (f *franzTopicLoggerWriter) disconnect() {
	if f.client == nil {
		return
	}
	if f.mgr != nil {
		_, _ = kafka.FranzSharedClientPop(SharedGlobalRedpandaClientKey, f.mgr)
	}
	f.client.Close()
	f.client = nil
}

func (f *franzTopicLoggerWriter) Close(context.Context) error {
	f.disconnect()
	return nil
}
