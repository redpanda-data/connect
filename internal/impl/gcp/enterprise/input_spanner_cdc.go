// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// Spanner Input Fields
const (
	siFieldProjectID         = "project_id"
	siFieldInstanceID        = "instance_id"
	siFieldDatabaseID        = "database_id"
	siFieldStreamID          = "stream_id"
	siFieldStartTimestamp    = "start_timestamp"
	siFieldEndTimestamp      = "end_timestamp"
	siFieldHeartbeatInterval = "heartbeat_interval"
)

type spannerCDCInputConfig struct {
	ProjectID         string
	InstanceID        string
	DatabaseID        string
	StreamID          string
	StartTimestamp    time.Time
	EndTimestamp      time.Time
	HeartbeatInterval time.Duration
}

func parseRFC3339Nano(pConf *service.ParsedConfig, key string) (time.Time, error) {
	s, err := pConf.FieldString(key)
	if err != nil {
		return time.Time{}, err
	}
	if s == "" {
		return time.Time{}, nil
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse %v as RFC3339Nano: %w", key, err)
	}
	return t, nil
}

func parseDuration(pConf *service.ParsedConfig, key string) (time.Duration, error) {
	s, err := pConf.FieldString(key)
	if err != nil {
		return 0, err
	}
	if s == "" {
		return 0, nil
	}

	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %v as duration: %w", key, err)
	}
	return d, nil
}

func spannerCDCInputConfigFromParsed(pConf *service.ParsedConfig) (conf spannerCDCInputConfig, err error) {
	if conf.ProjectID, err = pConf.FieldString(siFieldProjectID); err != nil {
		return
	}
	if conf.InstanceID, err = pConf.FieldString(siFieldInstanceID); err != nil {
		return
	}
	if conf.DatabaseID, err = pConf.FieldString(siFieldDatabaseID); err != nil {
		return
	}
	if conf.StreamID, err = pConf.FieldString(siFieldStreamID); err != nil {
		return
	}
	if conf.StartTimestamp, err = parseRFC3339Nano(pConf, siFieldStartTimestamp); err != nil {
		return
	}
	if conf.EndTimestamp, err = parseRFC3339Nano(pConf, siFieldEndTimestamp); err != nil {
		return
	}
	if conf.HeartbeatInterval, err = parseDuration(pConf, siFieldHeartbeatInterval); err != nil {
		return
	}
	return
}

func spannerCDCInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("TODO").
		Categories("Services", "GCP").
		Summary("Creates an input that consumes from a spanner change stream.").
		Description(`
Consumes change records from a Google Cloud Spanner change stream. This input allows
you to track and process database changes in real-time, making it useful for data
replication, event-driven architectures, and maintaining derived data stores.

The input reads from a specified change stream within a Spanner database and converts
each change record into a message. The message payload contains the change records in
JSON format, and metadata is added with details about the Spanner instance, database,
and stream.

Change streams provide a way to track mutations to your Spanner database tables. For
more information about Spanner change streams, refer to the Google Cloud documentation:
https://cloud.google.com/spanner/docs/change-streams
`).
		Field(service.NewStringField(siFieldProjectID).Description("GCP project ID containing the Spanner instance")).
		Field(service.NewStringField(siFieldInstanceID).Description("Spanner instance ID")).
		Field(service.NewStringField(siFieldDatabaseID).Description("Spanner database ID")).
		Field(service.NewStringField(siFieldStreamID).Description("The name of the change stream to track")).
		Field(service.NewStringField(siFieldStartTimestamp).Optional().Description("RFC3339 formatted timestamp to start reading from (default: current time)").Default("")).
		Field(service.NewStringField(siFieldEndTimestamp).Optional().Description("RFC3339 formatted timestamp to stop reading at (default: no end time)").Default("")).
		Field(service.NewStringField(siFieldHeartbeatInterval).Optional().Description("Duration string for heartbeat interval (e.g., '10s')").Default("10s")).
		Field(service.NewAutoRetryNacksToggleField())
}

func init() {
	service.MustRegisterInput("gcp_spanner_cdc", spannerCDCInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			r, err := newSpannerCDCReaderFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, r)
		})
}

//------------------------------------------------------------------------------

type spannerCDCReader struct {
	conf spannerCDCInputConfig
	log  *service.Logger

	reader    *changestreams.Reader
	cancel    context.CancelFunc
	resCh     chan *service.Message
	connected atomic.Bool
}

var _ service.Input = (*spannerCDCReader)(nil)

func newSpannerCDCReaderFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*spannerCDCReader, error) {
	if err := license.CheckRunningEnterprise(mgr); err != nil {
		return nil, err
	}

	conf, err := spannerCDCInputConfigFromParsed(pConf)
	if err != nil {
		return nil, err
	}
	return newSpannerCDCReader(conf, mgr)
}

func newSpannerCDCReader(conf spannerCDCInputConfig, mgr *service.Resources) (*spannerCDCReader, error) {
	r := &spannerCDCReader{
		conf: conf,
		log:  mgr.Logger(),
	}

	return r, nil
}

func (r *spannerCDCReader) onReadResult(res *changestreams.ReadResult) error {
	for _, cr := range res.ChangeRecords {
		for _, dcr := range cr.DataChangeRecords {
			b, err := json.Marshal(dcr)
			if err != nil {
				return fmt.Errorf("failed to marshal read result as JSON: %w", err)
			}

			msg := service.NewMessage(b)
			msg.MetaSetMut("spanner_partition_token", res.PartitionToken)
			msg.MetaSetMut("spanner_project_id", r.conf.ProjectID)
			msg.MetaSetMut("spanner_instance_id", r.conf.InstanceID)
			msg.MetaSetMut("spanner_database_id", r.conf.DatabaseID)
			msg.MetaSetMut("spanner_stream_id", r.conf.StreamID)

			r.resCh <- msg
		}
	}

	return nil
}

func (r *spannerCDCReader) Connect(ctx context.Context) error {
	r.log.Infof("Connecting to Spanner CDC stream: %s (project: %s, instance: %s, database: %s)",
		r.conf.StreamID, r.conf.ProjectID, r.conf.InstanceID, r.conf.DatabaseID)

	var err error
	r.reader, err = changestreams.NewReaderWithConfig(
		ctx,
		r.conf.ProjectID,
		r.conf.InstanceID,
		r.conf.DatabaseID,
		r.conf.StreamID,
		changestreams.Config{
			StartTimestamp:    r.conf.StartTimestamp,
			EndTimestamp:      r.conf.EndTimestamp,
			HeartbeatInterval: r.conf.HeartbeatInterval,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create Spanner change stream reader: %w", err)
	}

	r.resCh = make(chan *service.Message)

	var bg context.Context
	bg, r.cancel = context.WithCancel(ctx)
	go func() {
		r.connected.Store(true)
		if err := r.reader.Read(bg, r.onReadResult); err != nil {
			r.log.Errorf("Error reading from Spanner change stream: %v", err)
		} else {
			r.log.Debug("Spanner change stream reader finished")
		}
		r.connected.Store(false)
	}()

	return nil
}

func (r *spannerCDCReader) ack(ctx context.Context, err error) error {
	return nil
}

func (r *spannerCDCReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if !r.connected.Load() {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case res := <-r.resCh:
		return res, r.ack, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (r *spannerCDCReader) Close(ctx context.Context) error {
	r.cancel()
	r.reader.Close()
	return nil
}
