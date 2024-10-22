// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package snowflake

import (
	"context"
	"crypto/rsa"
	"fmt"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

const (
	ssoFieldAccount       = "account"
	ssoFieldUser          = "user"
	ssoFieldRole          = "role"
	ssoFieldDB            = "database"
	ssoFieldSchema        = "schema"
	ssoFieldTable         = "table"
	ssoFieldKey           = "private_key"
	ssoFieldKeyFile       = "private_key_file"
	ssoFieldKeyPass       = "private_key_pass"
	ssoFieldBatching      = "batching"
	ssoFieldChannelPrefix = "channel_prefix"
	ssoFieldMapping       = "mapping"
)

func snowflakeStreamingOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.39.0").
		Summary("Ingest data into Snowflake using Snowpipe Streaming.").
		Description(`
Ingest data into Snowflake using Snowpipe Streaming.

[%header,format=dsv]
|===
Snowflake column type:Allowed format in Redpanda Connect
CHAR, VARCHAR:string
BINARY:[]byte
NUMBER:any numeric type, string
FLOAT:any numeric type
BOOLEAN:bool,any numeric type,string parsable according to `+"`strconv.ParseBool`"+`
TIME,DATE,TIMESTAMP:unix or RFC 3339 with nanoseconds timestamps
VARIANT,ARRAY,OBJECT:any data type is converted into JSON
GEOGRAPHY,GEOMETRY: Not supported
|===

For TIMESTAMP, TIME and DATE columns, you can parse different string formats using a bloblang `+"`"+ssoFieldMapping+"`"+`.

Authentication can be configured using a https://docs.snowflake.com/en/user-guide/key-pair-auth[RSA Key Pair^].

There are https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#limitations[limitations^] of what data types can be loaded into Snowflake using this method.
`+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewStringField(ssoFieldAccount).
				Description(`Account name, which is the same as the https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#where-are-account-identifiers-used[Account Identifier^].
      However, when using an https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier[Account Locator^],
      the Account Identifier is formatted as `+"`<account_locator>.<region_id>.<cloud>`"+` and this field needs to be
      populated using the `+"`<account_locator>`"+` part.
`).Example("AAAAAAA-AAAAAAA"),
			service.NewStringField(ssoFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user."),
			service.NewStringField(ssoFieldRole).Description("The role for the `user` field. The role must have the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] to call the Snowpipe Streaming APIs. See https://docs.snowflake.com/en/user-guide/admin-user-management#user-roles[Snowflake Documentation^] for more information about roles.").Example("ACCOUNTADMIN"),
			service.NewStringField(ssoFieldDB).Description("The Snowflake database to ingest data into."),
			service.NewStringField(ssoFieldSchema).Description("The Snowflake schema to ingest data into."),
			service.NewStringField(ssoFieldTable).Description("The Snowflake table to ingest data into."),
			service.NewStringField(ssoFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").Optional().Secret(),
			service.NewStringField(ssoFieldKeyFile).Description("The file to load the private RSA key from. This should be a `.p8` PEM encoded file. Either this or `private_key` must be specified.").Optional(),
			service.NewStringField(ssoFieldKeyPass).Description("The RSA key passphrase if the RSA key is encrypted.").Optional().Secret(),
			service.NewBloblangField(ssoFieldMapping).Description("A bloblang mapping to execute on each message.").Optional(),
			service.NewBatchPolicyField(ssoFieldBatching),
			service.NewOutputMaxInFlightField(),
			service.NewStringField(ssoFieldChannelPrefix).
				Description(`The prefix to use when creating a channel name.
Duplicate channel names will result in errors and prevent multiple instances of Redpanda Connect from writing at the same time.
By default this will create a channel name that is based on the table FQN so there will only be a single stream per table.

NOTE: There is a limit of 10,000 streams per table - if using more than 10k streams please reach out to Snowflake support.`).
				Optional().
				Advanced(),
		).LintRule(`root = match {
  this.exists("private_key") && this.exists("private_key_file") => [ "both ` + "`private_key`" + ` and ` + "`private_key_file`" + ` can't be set simultaneously" ],
}`)
}

func init() {
	err := service.RegisterBatchOutput(
		"snowflake_streaming",
		snowflakeStreamingOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(ssoFieldBatching); err != nil {
				return
			}
			output, err = newSnowflakeStreamer(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

func newSnowflakeStreamer(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (service.BatchOutput, error) {
	keypass := ""
	if conf.Contains(ssoFieldKeyPass) {
		pass, err := conf.FieldString(ssoFieldKey)
		if err != nil {
			return nil, err
		}
		keypass = pass
	}
	var rsaKey *rsa.PrivateKey
	if conf.Contains(ssoFieldKey) {
		key, err := conf.FieldString(ssoFieldKey)
		if err != nil {
			return nil, err
		}
		rsaKey, err = getPrivateKey([]byte(key), keypass)
		if err != nil {
			return nil, err
		}
	} else if conf.Contains(ssoFieldKeyFile) {
		keyFile, err := conf.FieldString(ssoFieldKeyFile)
		if err != nil {
			return nil, err
		}
		rsaKey, err = getPrivateKeyFromFile(mgr.FS(), keyFile, keypass)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("one of `%s` or `%s` is required", ssoFieldKey, ssoFieldKeyFile)
	}
	account, err := conf.FieldString(ssoFieldAccount)
	if err != nil {
		return nil, err
	}
	user, err := conf.FieldString(ssoFieldUser)
	if err != nil {
		return nil, err
	}
	role, err := conf.FieldString(ssoFieldRole)
	if err != nil {
		return nil, err
	}
	db, err := conf.FieldString(ssoFieldDB)
	if err != nil {
		return nil, err
	}
	schema, err := conf.FieldString(ssoFieldSchema)
	if err != nil {
		return nil, err
	}
	table, err := conf.FieldString(ssoFieldTable)
	if err != nil {
		return nil, err
	}
	var mapping *bloblang.Executor
	if conf.Contains(ssoFieldMapping) {
		mapping, err = conf.FieldBloblang(ssoFieldMapping)
		if err != nil {
			return nil, err
		}
	}
	var channelPrefix string
	if conf.Contains(ssoFieldChannelPrefix) {
		channelPrefix, err = conf.FieldString(ssoFieldChannelPrefix)
		if err != nil {
			return nil, err
		}
	} else {
		// There is a limit of 10k channels, so we can't dynamically create them.
		// The only other good default is to create one and only allow a single
		// stream to write to a single table.
		channelPrefix = fmt.Sprintf("Redpanda_Connect_%s.%s.%s", db, schema, table)
	}
	client, err := streaming.NewSnowflakeServiceClient(
		context.Background(),
		streaming.ClientOptions{
			Account:        account,
			User:           user,
			Role:           role,
			PrivateKey:     rsaKey,
			Logger:         mgr.Logger(),
			ConnectVersion: mgr.EngineVersion(),
			Application:    strings.TrimPrefix(channelPrefix, "Redpanda_Connect_"),
		})
	if err != nil {
		return nil, err
	}
	o := &snowflakeStreamerOutput{
		channelPrefix:    channelPrefix,
		client:           client,
		db:               db,
		schema:           schema,
		table:            table,
		mapping:          mapping,
		logger:           mgr.Logger(),
		buildTime:        mgr.Metrics().NewTimer("snowflake_build_output_latency_ns"),
		uploadTime:       mgr.Metrics().NewTimer("snowflake_upload_latency_ns"),
		compressedOutput: mgr.Metrics().NewCounter("snowflake_compressed_output_size_bytes"),
	}
	return o, nil
}

type snowflakeStreamerOutput struct {
	client            *streaming.SnowflakeServiceClient
	channelPool       sync.Pool
	channelCreationMu sync.Mutex
	poolSize          int
	compressedOutput  *service.MetricCounter
	uploadTime        *service.MetricTimer
	buildTime         *service.MetricTimer

	channelPrefix, db, schema, table string
	mapping                          *bloblang.Executor
	logger                           *service.Logger
}

func (o *snowflakeStreamerOutput) openNewChannel(ctx context.Context) (*streaming.SnowflakeIngestionChannel, error) {
	// Use a lock here instead of an atomic because this should not be called at steady state and it's better to limit
	// creating extra channels when there is a limit of 10K.
	o.channelCreationMu.Lock()
	defer o.channelCreationMu.Unlock()
	name := fmt.Sprintf("%s_%d", o.channelPrefix, o.poolSize)
	client, err := o.openChannel(ctx, name, int16(o.poolSize))
	if err == nil {
		o.poolSize++
	}
	return client, err
}

func (o *snowflakeStreamerOutput) openChannel(ctx context.Context, name string, id int16) (*streaming.SnowflakeIngestionChannel, error) {
	o.logger.Debugf("opening snowflake streaming channel: %s", name)
	return o.client.OpenChannel(ctx, streaming.ChannelOptions{
		ID:           id,
		Name:         name,
		DatabaseName: o.db,
		SchemaName:   o.schema,
		TableName:    o.table,
	})
}

func (o *snowflakeStreamerOutput) Connect(ctx context.Context) error {
	// Precreate a single channel so we know stuff works, otherwise we'll create them on demand.
	c, err := o.openNewChannel(ctx)
	if err != nil {
		return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
	}
	o.channelPool.Put(c)
	return nil
}

func (o *snowflakeStreamerOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if o.mapping != nil {
		mapped := make(service.MessageBatch, len(batch))
		exec := batch.BloblangExecutor(o.mapping)
		for i := range batch {
			msg, err := exec.Query(i)
			if err != nil {
				return fmt.Errorf("error executing %s: %w", ssoFieldMapping, err)
			}
			mapped[i] = msg
		}
		batch = mapped
	}
	var channel *streaming.SnowflakeIngestionChannel
	if maybeChan := o.channelPool.Get(); maybeChan != nil {
		channel = maybeChan.(*streaming.SnowflakeIngestionChannel)
	} else {
		var err error
		if channel, err = o.openNewChannel(ctx); err != nil {
			return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
		}
	}
	stats, err := channel.InsertRows(ctx, batch)
	o.compressedOutput.Incr(int64(stats.CompressedOutputSize))
	o.uploadTime.Timing(stats.UploadTime.Nanoseconds())
	o.buildTime.Timing(stats.BuildTime.Nanoseconds())
	// If there is some kind of failure, try to reopen the channel
	if err != nil {
		reopened, reopenErr := o.openChannel(ctx, channel.Name, channel.ID)
		if reopenErr == nil {
			o.channelPool.Put(reopened)
		} else {
			o.logger.Warnf("unable to reopen channel %q after failure: %v", channel.Name, reopenErr)
			// Keep around the same channel just in case so we don't keep creating new channels.
			o.channelPool.Put(channel)
		}
		return err
	}
	polls, err := channel.WaitUntilCommitted(ctx)
	if err == nil {
		o.logger.Tracef("batch committed in snowflake after %d polls", polls)
	}
	o.channelPool.Put(channel)
	return err
}

func (o *snowflakeStreamerOutput) Close(ctx context.Context) error {
	return o.client.Close()
}
