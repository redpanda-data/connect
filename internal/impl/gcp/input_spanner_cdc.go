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

package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/spannercdc"
)

const (
	partitionDSN      string = "partition_dsn"
	streamDSN         string = "stream_dsn"
	streamID          string = "stream_id"
	partitionTable    string = "partition_table"
	startTimeEpoch    string = "start_time_epoch"
	useInMemPartition string = "use_in_memory_partition"
	allowedModTypes   string = "allowed_mod_types"
)

type streamReader interface {
	Stream(ctx context.Context, channel chan<- *spannercdc.DataChangeRecord) error
	Close() error
}

func newSpannerCDCInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.43.0").
		Categories("Services", "GCP").
		Summary("Creates an input that consumes from a spanner change stream.").
		Field(service.NewStringField(streamDSN).Description("Required field to use to connect to spanner for the change stream.").Example("projects/<project_id>/instances/<instance_id>/databases/<database_id>")).
		Field(service.NewStringField(streamID).Description("Required name of the change stream to track.").Default("")).
		Field(service.NewIntField(startTimeEpoch).Advanced().Optional().Default(0).Description("Optional microsecond accurate epoch timestamp to start reading from. If empty time.Now() will be used.")).
		Field(service.NewStringField(partitionDSN).Optional().Description("Field used to set the DSN for the metadata partition table, can be the same as stream_dsn.").Example("projects/<project_id>/instances/<instance_id>/databases/<database_id>")).
		Field(service.NewStringField(partitionTable).Optional().Description("Name of the table to create/use in spanner to track change stream partition metadata.")).
		Field(service.NewBoolField(useInMemPartition).Description("use an in memory partition table for tracking the partitions.").Default(false)).
		Field(service.NewStringListField(allowedModTypes).Advanced().Description("Mod types to allow through when reading the change stream, default all.").Default([]string{spannercdc.ModTypeINSERT, spannercdc.ModTypeUPDATE, spannercdc.ModTypeDELETE}))
}

func newSpannerStreamInput(conf *service.ParsedConfig, log *service.Logger) (out *spannerStreamInput, err error) {
	out = &spannerStreamInput{
		// not buffered to prevent the cursor from getting too far ahead.
		// there is still the chance that we could lose changes though.
		changeChannel: make(chan *spannercdc.DataChangeRecord, 1),
		log:           log,
		stopSig:       shutdown.NewSignaller(),
	}
	if out.partitionDSN, err = conf.FieldString(partitionDSN); err != nil {
		return nil, err
	}

	if out.partitionTable, err = conf.FieldString(partitionTable); err != nil {
		return nil, err
	}

	if out.streamDSN, err = conf.FieldString(streamDSN); err != nil {
		return nil, err
	}

	if out.streamID, err = conf.FieldString(streamID); err != nil {
		return nil, err
	}

	if out.allowedModTypes, err = conf.FieldStringList(allowedModTypes); err != nil {
		return nil, err
	}
	for _, modType := range out.allowedModTypes {
		if !slices.ContainsFunc(spannercdc.AllModTypes, func(s spannercdc.ModType) bool {
			return modType == string(s)
		}) {
			err = errors.New("allowed_mod_types must be one of INSERT, UPDATE, DELETE")
			return nil, err
		}
	}

	useInMemPartition, err := conf.FieldBool(useInMemPartition)
	if err != nil {
		return nil, err
	}

	startTimeEpoch, err := conf.FieldInt(startTimeEpoch)
	if err != nil {
		return nil, err
	}

	if startTimeEpoch > 0 {
		out.startTime = func(seconds int) *time.Time {
			t := time.UnixMicro(int64(startTimeEpoch))
			return &t
		}(startTimeEpoch)
	}

	if !useInMemPartition && slices.Contains([]string{out.partitionDSN, out.partitionTable, out.streamDSN, out.streamID}, "") {
		return nil, fmt.Errorf("%s, %s, %s, and %s must be set", partitionDSN, partitionTable, streamDSN, streamID)
	} else if slices.Contains([]string{out.streamDSN, out.streamID}, "") {
		return nil, fmt.Errorf("%s, and %s must be set", streamDSN, streamID)
	}
	out.useInMemPartition = useInMemPartition

	return
}

func init() {
	err := service.RegisterInput(
		"gcp_spanner_cdc", newSpannerCDCInputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			streamInput, err := newSpannerStreamInput(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return conf.WrapInputExtractTracingSpanMapping("gcp_spanner_cdc", service.AutoRetryNacks(streamInput))
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type spannerStreamInput struct {
	streamDSN         string
	streamClient      *db
	streamID          string
	partitionDSN      string
	partitionTable    string
	useInMemPartition bool
	startTime         *time.Time
	allowedModTypes   []string
	reader            streamReader
	// create a channel to pass from connection to read.
	changeChannel chan *spannercdc.DataChangeRecord
	log           *service.Logger
	stopSig       *shutdown.Signaller
}

func (i *spannerStreamInput) Connect(ctx context.Context) (err error) {
	i.stopSig = shutdown.NewSignaller()
	jobctx, _ := i.stopSig.SoftStopCtx(context.Background())

	if i.streamClient == nil {
		i.streamClient, err = newDatabase(jobctx, i.streamDSN)
		if err != nil {
			return err
		}
	}
	if i.reader == nil {
		i.reader, err = newStreamer(jobctx, i.streamClient, i.streamID, i.partitionDSN, i.partitionTable, i.useInMemPartition, i.allowedModTypes, i.startTime)
		if err != nil {
			return err
		}
	}
	go func() {
		if rerr := i.reader.Stream(jobctx, i.changeChannel); rerr != nil {
			i.log.Errorf("Subscription error: %v\n", rerr)
			close(i.changeChannel)
		}
	}()
	return nil
}

func (i *spannerStreamInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case msg := <-i.changeChannel:
		data, err := json.Marshal(msg)
		if err != nil {
			return nil, nil, err
		}
		messageOut := service.NewMessage(data)
		messageOut.MetaSet("tabe", msg.TableName)
		messageOut.MetaSet("transaction_tag", msg.TransactionTag)
		messageOut.MetaSet("mod_type", string(msg.ModType))
		messageOut.MetaSet("commit_timestamp", msg.CommitTimestamp.Format(time.RFC3339Nano))
		return messageOut, func(ctx context.Context, err error) error {
			// Nacks are retried automatically when we use service.AutoRetryNacks
			return nil
		}, nil
	case <-i.stopSig.HasStoppedChan():
		return nil, nil, service.ErrNotConnected
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (i *spannerStreamInput) Close(_ context.Context) error {
	if i.reader != nil {
		return i.reader.Close()
	}
	return nil
}

// -------------------------

type streamerDB struct {
	streamID, partitionTable            string
	changeStreamClient, partitionClient *db
	subscriber                          *spannercdc.Subscriber
	allowedModTypes                     []string
}

func newStreamer(ctx context.Context,
	changestreamClient *db,
	streamID, partitionDSN, partitionTable string,
	useInMemPartition bool,
	modTypes []string,
	startTime *time.Time,
) (streamReader, error) {
	streamer := &streamerDB{
		streamID:           streamID,
		partitionTable:     partitionTable,
		allowedModTypes:    modTypes,
		changeStreamClient: changestreamClient,
	}

	var pStorage spannercdc.PartitionStorage = spannercdc.NewInmemory()
	// only use DB meta partition table if explicitly enabled.
	if !useInMemPartition {
		partitionClient, err := newDatabase(ctx, partitionDSN)
		if err != nil {
			return nil, err
		}
		streamer.partitionClient = partitionClient

		spannerPartitionStorage := spannercdc.NewSpanner(partitionClient.client, partitionTable)
		if err := spannerPartitionStorage.CreateTableIfNotExists(ctx); err != nil {
			return nil, err
		}
		// assign here as we need to use the partition storage in the subscriber.
		pStorage = spannerPartitionStorage
	}

	options := []spannercdc.Option{}
	// if provided with a specific startime. use that.
	if startTime != nil {
		options = append(options, spannercdc.WithStartTimestamp(*startTime))
	}
	subscriber := spannercdc.NewSubscriber(streamer.changeStreamClient.client, streamID, pStorage, options...)

	streamer.subscriber = subscriber

	return streamer, nil
}

// Stream provides a stream of change records from a Spanner database configured stream to your provided channel.
// Stream is blocking unless the provided context is cancelled or an error occurs.
func (s *streamerDB) Stream(ctx context.Context, channel chan<- *spannercdc.DataChangeRecord) error {
	return s.subscriber.SubscribeFunc(ctx, func(dcr *spannercdc.DataChangeRecord) error {
		if slices.Contains(s.allowedModTypes, string(dcr.ModType)) {
			channel <- dcr
		}
		return nil
	})
}

func (s *streamerDB) Close() error {
	if s.changeStreamClient != nil {
		s.changeStreamClient.Close()
	}
	if s.partitionClient != nil {
		s.partitionClient.Close()
	}
	return nil
}

//  -----------------

type db struct {
	client *spanner.Client
}

func newDatabase(ctx context.Context, dsn string) (*db, error) {
	spannerClient, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("spanner.NewClient: %w", err)
	}

	return &db{
		client: spannerClient,
	}, nil
}

func (s *db) Close() error {
	if s.client != nil {
		s.client.Close()
	}
	return nil
}
