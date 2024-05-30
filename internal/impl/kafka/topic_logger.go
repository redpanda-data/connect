// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TopicLoggerFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewStringListField("seed_brokers").
			Description("A list of broker addresses to connect to in order to establish connections. If an item of the list contains commas it will be expanded into multiple addresses.").
			Optional().
			Example([]string{"localhost:9092"}).
			Example([]string{"foo:9092", "bar:9092"}).
			Example([]string{"foo:9092,bar:9092"}),
		service.NewStringField("logs_topic").
			Default("__redpanda.connect.logs"),
		service.NewStringEnumField("logs_level", "debug", "info", "warn", "error").
			Default("info"),
		service.NewStringField("client_id").
			Description("An identifier for the client connection.").
			Default("benthos").
			Advanced(),
		service.NewStringField("rack_id").
			Description("A rack identifier for this client.").
			Default("").
			Advanced(),
		service.NewDurationField("timeout").
			Description("The maximum period of time to wait for message sends before abandoning the request and retrying").
			Default("10s").
			Advanced(),
		service.NewStringField("max_message_bytes").
			Description("The maximum space in bytes than an individual message may take, messages larger than this value will be rejected. This field corresponds to Kafka's `max.message.bytes`.").
			Advanced().
			Default("1MB").
			Example("100MB").
			Example("50mib"),
		service.NewTLSToggledField("tls"),
		saslField(),
	}
}

// TopicLogger provides a mechanism for sending service-wide logs into a kafka
// topic. The writing is done by a regular output, but this type is necessary in
// order to allow hot swapping of log components during start up.
type TopicLogger struct {
	fallbackLogger *atomic.Pointer[service.Logger]
	o              *atomic.Pointer[service.OwnedOutput]
	level          *atomic.Pointer[slog.Level]
	attrs          []slog.Attr
}

func NewTopicLogger() *TopicLogger {
	return &TopicLogger{
		fallbackLogger: &atomic.Pointer[service.Logger]{},
		o:              &atomic.Pointer[service.OwnedOutput]{},
		level:          &atomic.Pointer[slog.Level]{},
	}
}

func (l *TopicLogger) SetFallbackLogger(fLogger *service.Logger) {
	l.fallbackLogger.Store(fLogger)
}

func (l *TopicLogger) InitOutputFromParsed(pConf *service.ParsedConfig) error {
	w, err := newTopicLoggerWriterFromConfig(pConf, l.fallbackLogger.Load())
	if err != nil {
		return err
	}
	if w == nil {
		return nil
	}

	lvlStr, err := pConf.FieldString("logs_level")
	if err != nil {
		return err
	}

	var lvl slog.Level
	switch strings.ToLower(lvlStr) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return fmt.Errorf("log level not recognized: %v", lvlStr)
	}
	l.level.Store(&lvl)

	res := service.MockResources(service.MockResourcesOptUseLogger(l.fallbackLogger.Load()))
	tmpO, err := res.ManagedBatchOutput("redpanda_logger", 24, w)
	if err != nil {
		return err
	}

	batchPol, err := (service.BatchPolicy{
		Count:  50,
		Period: "1s",
	}).NewBatcher(service.MockResources())
	if err != nil {
		return err
	}

	tmpO = tmpO.BatchedWith(batchPol)
	if err := tmpO.PrimeBuffered(100); err == nil {
		l.o.Store(tmpO)
	} else {
		l.fallbackLogger.Load().With("error", err.Error()).Warn("failed to initialise topic logs writer")
	}
	return nil
}

func (l *TopicLogger) Enabled(ctx context.Context, atLevel slog.Level) bool {
	lvl := l.level.Load()
	if lvl == nil {
		return true
	}
	return atLevel >= *lvl
}

func (l *TopicLogger) Handle(ctx context.Context, r slog.Record) error {
	tmpO := l.o.Load()
	if tmpO == nil {
		return nil
	}

	lvl := l.level.Load()
	if lvl == nil || r.Level < *lvl {
		return nil
	}

	msg := service.NewMessage(nil)

	v := map[string]any{
		"message": r.Message,
		"level":   r.Level.String(),
		"time":    r.Time.Format(time.RFC3339Nano),
	}
	for _, a := range l.attrs {
		v[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		v[a.Key] = a.Value.String()
		return true
	})
	msg.SetStructured(v)
	_ = tmpO.WriteBatchNonBlocking(service.MessageBatch{msg}, func(ctx context.Context, err error) error {
		return nil // TODO: Log nacks
	}) // TODO: Log errors (occasionally)
	return nil
}

func (l *TopicLogger) WithAttrs(attrs []slog.Attr) slog.Handler {
	newL := *l
	newAttributes := make([]slog.Attr, 0, len(attrs)+len(l.attrs))
	newAttributes = append(newAttributes, l.attrs...)
	newAttributes = append(newAttributes, attrs...)
	newL.attrs = newAttributes
	return &newL
}

func (l *TopicLogger) WithGroup(name string) slog.Handler {
	return l // TODO
}

//------------------------------------------------------------------------------

type franzTopicLoggerWriter struct {
	topic string

	seedBrokers      []string
	clientID         string
	rackID           string
	tlsConf          *tls.Config
	saslConfs        []sasl.Mechanism
	partitioner      kgo.Partitioner
	timeout          time.Duration
	produceMaxBytes  int32
	compressionPrefs []kgo.CompressionCodec

	client *kgo.Client

	log *service.Logger
}

func newTopicLoggerWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*franzTopicLoggerWriter, error) {
	f := franzTopicLoggerWriter{
		log: log,
	}

	if !conf.Contains("seed_brokers") {
		return nil, nil
	}

	brokerList, err := conf.FieldStringList("seed_brokers")
	if err != nil {
		return nil, err
	}
	for _, b := range brokerList {
		f.seedBrokers = append(f.seedBrokers, strings.Split(b, ",")...)
	}
	if len(brokerList) == 0 {
		return nil, nil
	}

	if f.topic, err = conf.FieldString("logs_topic"); err != nil {
		return nil, err
	}
	if f.topic == "" {
		return nil, nil
	}

	if f.timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	maxBytesStr, err := conf.FieldString("max_message_bytes")
	if err != nil {
		return nil, err
	}
	maxBytes, err := humanize.ParseBytes(maxBytesStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse max_message_bytes: %w", err)
	}
	if maxBytes > uint64(math.MaxInt32) {
		return nil, fmt.Errorf("invalid max_message_bytes, must not exceed %v", math.MaxInt32)
	}
	f.produceMaxBytes = int32(maxBytes)

	if conf.Contains("compression") {
		cStr, err := conf.FieldString("compression")
		if err != nil {
			return nil, err
		}

		var c kgo.CompressionCodec
		switch cStr {
		case "lz4":
			c = kgo.Lz4Compression()
		case "gzip":
			c = kgo.GzipCompression()
		case "snappy":
			c = kgo.SnappyCompression()
		case "zstd":
			c = kgo.ZstdCompression()
		case "none":
			c = kgo.NoCompression()
		default:
			return nil, fmt.Errorf("compression codec %v not recognised", cStr)
		}
		f.compressionPrefs = append(f.compressionPrefs, c)
	}

	f.partitioner = kgo.StickyKeyPartitioner(nil)
	if conf.Contains("partitioner") {
		partStr, err := conf.FieldString("partitioner")
		if err != nil {
			return nil, err
		}
		switch partStr {
		case "murmur2_hash":
			f.partitioner = kgo.StickyKeyPartitioner(nil)
		case "round_robin":
			f.partitioner = kgo.RoundRobinPartitioner()
		case "least_backup":
			f.partitioner = kgo.LeastBackupPartitioner()
		case "manual":
			f.partitioner = kgo.ManualPartitioner()
		default:
			return nil, fmt.Errorf("unknown partitioner: %v", partStr)
		}
	}

	if f.clientID, err = conf.FieldString("client_id"); err != nil {
		return nil, err
	}

	if f.rackID, err = conf.FieldString("rack_id"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		f.tlsConf = tlsConf
	}
	if f.saslConfs, err = saslMechanismsFromConfig(conf); err != nil {
		return nil, err
	}

	return &f, nil
}

//------------------------------------------------------------------------------

func (f *franzTopicLoggerWriter) Connect(ctx context.Context) error {
	if f.client != nil {
		return nil
	}

	clientOpts := []kgo.Opt{
		kgo.SeedBrokers(f.seedBrokers...),
		kgo.SASL(f.saslConfs...),
		kgo.AllowAutoTopicCreation(), // TODO: Configure this
		kgo.ProducerBatchMaxBytes(f.produceMaxBytes),
		kgo.ProduceRequestTimeout(f.timeout),
		kgo.ClientID(f.clientID),
		kgo.Rack(f.rackID),
		kgo.WithLogger(&kgoLogger{f.log}),
	}
	if f.tlsConf != nil {
		clientOpts = append(clientOpts, kgo.DialTLSConfig(f.tlsConf))
	}
	if f.partitioner != nil {
		clientOpts = append(clientOpts, kgo.RecordPartitioner(f.partitioner))
	}
	if len(f.compressionPrefs) > 0 {
		clientOpts = append(clientOpts, kgo.ProducerBatchCompression(f.compressionPrefs...))
	}

	cl, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return err
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
		record := &kgo.Record{Topic: f.topic}
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
	f.client.Close()
	f.client = nil
}

func (f *franzTopicLoggerWriter) Close(ctx context.Context) error {
	f.disconnect()
	return nil
}
