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
	"github.com/anicoll/screamer/pkg/model"
	"github.com/anicoll/screamer/pkg/partitionstorage"
	"github.com/anicoll/screamer/pkg/screamer"
	"github.com/redpanda-data/benthos/v4/public/service"
)

type streamReader interface {
	Stream(ctx context.Context, channel chan<- *model.DataChangeRecord) error
	Close() error
}

func newSpannerChangeStreamInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Version("3.43.0").
		Categories("Services", "GCP").
		Summary("Creates an input that consumes from a spanner change stream.").
		Field(service.NewStringField("partition_dsn")).
		Field(service.NewStringField("partition_table")).
		Field(service.NewBoolField("use_in_mememory_partition").Description("use an in memory partition table for tracking the partitions").Default(false)).
		Field(service.NewStringField("stream_dsn").Optional().Default("")).
		Field(service.NewStringField("stream_id").Description("The name of the change stream to track").Default("")).
		Field(service.NewIntField("start_time_epoch").Optional().Description("Microsecond accurate epoch timestamp to start reading from").Default(0)).
		Field(service.NewStringListField("allowed_mod_types").Default([]string{"INSERT", "UPDATE", "DELETE"}))
}

func newSpannerStreamInput(conf *service.ParsedConfig, log *service.Logger) (out *spannerStreamInput, err error) {
	out = &spannerStreamInput{
		// not buffered to prevent the cursor from getting too far ahead.
		// there is still the chance that we could lose changes though.
		changeChannel: make(chan *model.DataChangeRecord, 1),
		log:           log,
		shutdownSig:   shutdown.NewSignaller(),
	}
	out.partitionDSN, err = conf.FieldString("partition_dsn")
	if err != nil {
		return
	}

	out.partitionTable, err = conf.FieldString("partition_table")
	if err != nil {
		return
	}

	out.streamDSN, err = conf.FieldString("stream_dsn")
	if err != nil {
		return
	}

	out.streamID, err = conf.FieldString("stream_id")
	if err != nil {
		return
	}

	useInMemPartition, err := conf.FieldBool("use_in_mememory_partition")
	if err != nil {
		return
	}

	startTimeEpoch, err := conf.FieldInt("start_time_epoch")
	if err != nil {
		return
	}

	if startTimeEpoch > 0 {
		out.startTime = func(seconds int) *time.Time {
			t := time.UnixMicro(int64(startTimeEpoch))
			return &t
		}(startTimeEpoch)
	}

	out.allowedModTypes, err = conf.FieldStringList("allowed_mod_types")
	if err != nil {
		return
	}
	if !useInMemPartition && slices.Contains([]string{out.partitionDSN, out.partitionTable, out.streamDSN, out.streamID}, "") {
		return nil, errors.New("partition_dsn, partition_table, stream_dsn, and stream_id must be set")
	} else if slices.Contains([]string{out.streamDSN, out.streamID}, "") {
		return nil, errors.New("stream_dsn, and stream_id must be set")
	}
	out.usePartitionTable = !useInMemPartition
	return
}

func init() {
	err := service.RegisterInput(
		"spanner_change_stream", newSpannerChangeStreamInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newSpannerStreamInput(conf, mgr.Logger())
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
	usePartitionTable bool
	startTime         *time.Time
	allowedModTypes   []string
	reader            streamReader
	// create a channel to pass from connection to read.
	changeChannel chan *model.DataChangeRecord

	log         *service.Logger
	shutdownSig *shutdown.Signaller
}

func (i *spannerStreamInput) Connect(ctx context.Context) (err error) {
	jobctx, _ := i.shutdownSig.SoftStopCtx(context.Background())

	if i.streamClient == nil {
		i.streamClient, err = newDatabase(jobctx, i.streamDSN)
		if err != nil {
			return err
		}
	}
	if i.reader == nil {
		i.reader, err = newStreamer(jobctx, i.streamClient, i.streamID, i.partitionDSN, i.partitionTable, i.usePartitionTable, i.allowedModTypes, i.startTime)
		if err != nil {
			return err
		}
	}
	go func() {
		if rerr := i.reader.Stream(jobctx, i.changeChannel); rerr != nil {
			i.log.Errorf("Subscription error: %v\n", rerr)
			close(i.changeChannel)
			panic(rerr)
		}
	}()
	return nil
}

func (i *spannerStreamInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	msg := <-i.changeChannel
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}
	return service.NewMessage(data), func(ctx context.Context, err error) error {
		// Nacks are retried automatically when we use service.AutoRetryNacks
		return nil
	}, nil
}

func (i *spannerStreamInput) Close(_ context.Context) error {
	close(i.changeChannel)
	if i.reader != nil {
		return i.reader.Close()
	}
	return nil
}

// -------------------------

type streamerDB struct {
	streamID, partitionTable            string
	changeStreamClient, partitionClient *db
	subscriber                          *screamer.Subscriber
	allowedModTypes                     []string
}

func newStreamer(ctx context.Context,
	changestreamClient *db,
	streamID, partitionDSN, partitionTable string,
	usePartitionTable bool,
	modTypes []string,
	startTime *time.Time,
) (streamReader, error) {
	streamer := &streamerDB{
		streamID:           streamID,
		partitionTable:     partitionTable,
		allowedModTypes:    modTypes,
		changeStreamClient: changestreamClient,
	}

	var pStorage screamer.PartitionStorage = partitionstorage.NewInmemory()
	// only use DB meta partition table if explicitly enabled.
	if usePartitionTable {
		partitionClient, err := newDatabase(ctx, partitionDSN)
		if err != nil {
			return nil, err
		}
		streamer.partitionClient = partitionClient

		spannerPartitionStorage := partitionstorage.NewSpanner(partitionClient.client, partitionTable)
		if err := spannerPartitionStorage.CreateTableIfNotExists(ctx); err != nil {
			return nil, err
		}
		// assign here as we need to use the partition storage in the subscriber.
		pStorage = spannerPartitionStorage
	}

	options := []screamer.Option{}
	// if provided with a specific startime. use that.
	if startTime != nil {
		options = append(options, screamer.WithStartTimestamp(*startTime))
	}
	subscriber := screamer.NewSubscriber(streamer.changeStreamClient.client, streamID, pStorage, options...)

	streamer.subscriber = subscriber

	return streamer, nil
}

// Stream provides a stream of change records from a Spanner database configured stream to your provided channel.
// Stream is blocking unless the provided context is cancelled or an error occurs.
func (s *streamerDB) Stream(ctx context.Context, channel chan<- *model.DataChangeRecord) error {
	return s.subscriber.SubscribeFunc(ctx, func(dcr *model.DataChangeRecord) error {
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
