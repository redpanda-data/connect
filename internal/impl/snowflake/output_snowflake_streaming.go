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
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
)

const (
	ssoFieldAccount                             = "account"
	ssoFieldUser                                = "user"
	ssoFieldRole                                = "role"
	ssoFieldDB                                  = "database"
	ssoFieldSchema                              = "schema"
	ssoFieldTable                               = "table"
	ssoFieldKey                                 = "private_key"
	ssoFieldKeyFile                             = "private_key_file"
	ssoFieldKeyPass                             = "private_key_pass"
	ssoFieldInitStatement                       = "init_statement"
	ssoFieldBatching                            = "batching"
	ssoFieldChannelPrefix                       = "channel_prefix"
	ssoFieldMapping                             = "mapping"
	ssoFieldBuildParallelism                    = "build_parallelism"
	ssoFieldSchemaEvolution                     = "schema_evolution"
	ssoFieldSchemaEvolutionEnabled              = "enabled"
	ssoFieldSchemaEvolutionNewColumnTypeMapping = "new_column_type_mapping"

	defaultSchemaEvolutionNewColumnMapping = `root = match this.value.type() {
  this == "string" => "STRING"
  this == "bytes" => "BINARY"
  this == "number" => "DOUBLE"
  this == "bool" => "BOOLEAN"
  this == "timestamp" => "TIMESTAMP"
  _ => "VARIANT"
}`
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
`+service.OutputPerformanceDocs(true, true)+`

It is recommended that each batches results in at least 16MiB of compressed output being written to Snowflake.
You can monitor the output batch size using the `+"`snowflake_compressed_output_size_bytes`"+` metric.
`).
		Fields(
			service.NewStringField(ssoFieldAccount).
				Description(`The Snowflake https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier[Account name^]. Which should be formatted as `+"`<orgname>-<account_name>`"+` where `+"`<orgname>`"+` is the name of your Snowflake organization and `+"`<account_name>`"+` is the unique name of your account within your organization.
`).Example("ORG-ACCOUNT"),
			service.NewStringField(ssoFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user."),
			service.NewStringField(ssoFieldRole).Description("The role for the `user` field. The role must have the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] to call the Snowpipe Streaming APIs. See https://docs.snowflake.com/en/user-guide/admin-user-management#user-roles[Snowflake Documentation^] for more information about roles.").Example("ACCOUNTADMIN"),
			service.NewStringField(ssoFieldDB).Description("The Snowflake database to ingest data into."),
			service.NewStringField(ssoFieldSchema).Description("The Snowflake schema to ingest data into."),
			service.NewStringField(ssoFieldTable).Description("The Snowflake table to ingest data into."),
			service.NewStringField(ssoFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").Optional().Secret(),
			service.NewStringField(ssoFieldKeyFile).Description("The file to load the private RSA key from. This should be a `.p8` PEM encoded file. Either this or `private_key` must be specified.").Optional(),
			service.NewStringField(ssoFieldKeyPass).Description("The RSA key passphrase if the RSA key is encrypted.").Optional().Secret(),
			service.NewBloblangField(ssoFieldMapping).Description("A bloblang mapping to execute on each message.").Optional(),
			service.NewStringField(ssoFieldInitStatement).Description(`
Optional SQL statements to execute immediately upon the first connection. This is a useful way to initialize tables before processing data. Care should be taken to ensure that the statement is idempotent, and therefore would not cause issues when run multiple times after service restarts.
`).Optional().Example(`
CREATE TABLE IF NOT EXISTS mytable (amount NUMBER);
`).Example(`
ALTER TABLE t1 ALTER COLUMN c1 DROP NOT NULL;
ALTER TABLE t1 ADD COLUMN a2 NUMBER;
`),
			service.NewObjectField(ssoFieldSchemaEvolution,
				service.NewBoolField(ssoFieldSchemaEvolutionEnabled).Description("Whether schema evolution is enabled."),
				service.NewBloblangField(ssoFieldSchemaEvolutionNewColumnTypeMapping).Description(`
The mapping function from Redpanda Connect type to column type in Snowflake. Overriding this can allow for customization of the datatype if there is specific information that you know about the data types in use. This mapping should result in the `+"`root`"+` variable being assigned a string with the data type for the new column in Snowflake.

The input to this mapping is an object with the value and the name of the new column, for example: `+"`"+`{"value": 42.3, "name":"new_data_field"}`+`"`).Default(defaultSchemaEvolutionNewColumnMapping),
			).Description(`Options to control schema evolution within the pipeline as new columns are added to the pipeline.`).Optional(),
			service.NewIntField(ssoFieldBuildParallelism).Description("The maximum amount of parallelism to use when building the output for Snowflake. The metric to watch to see if you need to change this is `snowflake_build_output_latency_ns`.").Default(1).Advanced(),
			service.NewBatchPolicyField(ssoFieldBatching),
			service.NewOutputMaxInFlightField(),
			service.NewStringField(ssoFieldChannelPrefix).
				Description(`The prefix to use when creating a channel name.
Duplicate channel names will result in errors and prevent multiple instances of Redpanda Connect from writing at the same time.
By default this will create a channel name that is based on the table FQN so there will only be a single stream per table.

At most `+"`max_in_flight`"+` channels will be opened.

NOTE: There is a limit of 10,000 streams per table - if using more than 10k streams please reach out to Snowflake support.`).
				Optional().
				Advanced(),
		).LintRule(`root = match {
  this.exists("private_key") && this.exists("private_key_file") => [ "both `+"`private_key`"+` and `+"`private_key_file`"+` can't be set simultaneously" ],
}`).
		Example(
			"Ingesting data from Redpanda",
			`How to ingest data from Redpanda with consumer groups, decode the schema using the schema registry, then write the corresponding data into Snowflake.`,
			`
input:
  kafka_franz:
    seed_brokers: ["redpanda.example.com:9092"]
    topics: ["my_topic_going_to_snow"]
    consumer_group: "redpanda_connect_to_snowflake"
    tls: {enabled: true}
    sasl:
      - mechanism: SCRAM-SHA-256
        username: MY_USER_NAME
        password: "${TODO}"
pipeline:
  processors:
    - schema_registry_decode:
        url: "redpanda.example.com:8081"
        basic_auth:
          enabled: true
          username: MY_USER_NAME
          password: "${TODO}"
output:
  snowflake_streaming:
    # By default there is only a single channel per output table allowed
    # if we want to have multiple Redpanda Connect streams writing data
    # then we need a unique channel prefix per stream. We'll use the host
    # name to get unique prefixes in this example.
    channel_prefix: "snowflake-channel-for-${HOST}"
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: ACCOUNTADMIN
    database: "MYDATABASE"
    schema: "PUBLIC"
    table: "MYTABLE"
    private_key_file: "my/private/key.p8"
    schema_evolution:
      enabled: true
`,
		).
		Example(
			"HTTP Sidecar to push data to Snowflake",
			`This example demonstrates how to create an HTTP server input that can recieve HTTP PUT requests
with JSON payloads, that are buffered locally then written to Snowflake in batches.

NOTE: This example uses a buffer to respond to the HTTP request immediately, so it's possible that failures to deliver data could result in data loss.
See the documentation about xref:components:buffers/memory.adoc[buffers] for more information, or remove the buffer entirely to respond to the HTTP request only once the data is written to Snowflake.`,
			`
input:
  http_server:
    path: /snowflake
buffer:
  memory:
    # Max inflight data before applying backpressure
    limit: 524288000 # 50MiB
    # Batching policy, influences how large the generated files sent to Snowflake are
    batch_policy:
      enabled: true
      byte_size: 33554432 # 32MiB
      period: "10s"
output:
  snowflake_streaming:
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: ACCOUNTADMIN
    database: "MYDATABASE"
    schema: "PUBLIC"
    table: "MYTABLE"
    private_key_file: "my/private/key.p8"
`,
		)
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
		pass, err := conf.FieldString(ssoFieldKeyPass)
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
	var schemaEvolutionMapping *bloblang.Executor
	if conf.Contains(ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionEnabled) {
		enabled, err := conf.FieldBool(ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionEnabled)
		if err == nil && enabled {
			schemaEvolutionMapping, err = conf.FieldBloblang(ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionNewColumnTypeMapping)
		}
		if err != nil {
			return nil, err
		}
	}

	buildParallelism, err := conf.FieldInt(ssoFieldBuildParallelism)
	if err != nil {
		return nil, err
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

	// Normalize role, db and schema as they are case-sensitive in the API calls.
	// Maybe we should use the golang SQL driver for SQL statements so we don't have
	// to handle this, instead of the REST API directly.
	role = strings.ToUpper(role)
	db = strings.ToUpper(db)
	schema = strings.ToUpper(schema)

	var initStatementsFn func(context.Context, *streaming.SnowflakeRestClient) error
	if conf.Contains(ssoFieldInitStatement) {
		initStatements, err := conf.FieldString(ssoFieldInitStatement)
		if err != nil {
			return nil, err
		}
		initStatementsFn = func(ctx context.Context, client *streaming.SnowflakeRestClient) error {
			_, err = client.RunSQL(ctx, streaming.RunSQLRequest{
				Statement: initStatements,
				// Currently we set of timeout of 30 seconds so that we don't have to handle async operations
				// that need polling to wait until they finish (results are made async when execution is longer
				// than 45 seconds).
				Timeout:  30,
				Database: db,
				Schema:   schema,
				Role:     role,
				// Auto determine the number of statements
				Parameters: map[string]string{
					"MULTI_STATEMENT_COUNT": "0",
				},
			})
			return err
		}
	}
	restClient, err := streaming.NewRestClient(account, user, mgr.EngineVersion(), channelPrefix, rsaKey, mgr.Logger())
	if err != nil {
		return nil, fmt.Errorf("unable to create rest API client: %w", err)
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
			Application:    channelPrefix,
		})
	if err != nil {
		return nil, err
	}
	o := &snowflakeStreamerOutput{
		channelPrefix:          channelPrefix,
		client:                 client,
		db:                     db,
		schema:                 schema,
		table:                  table,
		role:                   role,
		mapping:                mapping,
		logger:                 mgr.Logger(),
		buildTime:              mgr.Metrics().NewTimer("snowflake_build_output_latency_ns"),
		uploadTime:             mgr.Metrics().NewTimer("snowflake_upload_latency_ns"),
		convertTime:            mgr.Metrics().NewTimer("snowflake_convert_latency_ns"),
		serializeTime:          mgr.Metrics().NewTimer("snowflake_serialize_latency_ns"),
		compressedOutput:       mgr.Metrics().NewCounter("snowflake_compressed_output_size_bytes"),
		initStatementsFn:       initStatementsFn,
		buildParallelism:       buildParallelism,
		schemaEvolutionMapping: schemaEvolutionMapping,
		restClient:             restClient,
	}
	return o, nil
}

type snowflakeStreamerOutput struct {
	client                 *streaming.SnowflakeServiceClient
	channelPool            sync.Pool
	channelCreationMu      sync.Mutex
	poolSize               int
	compressedOutput       *service.MetricCounter
	uploadTime             *service.MetricTimer
	buildTime              *service.MetricTimer
	convertTime            *service.MetricTimer
	serializeTime          *service.MetricTimer
	buildParallelism       int
	schemaEvolutionMapping *bloblang.Executor

	schemaMigrationMu                      sync.RWMutex
	channelPrefix, db, schema, table, role string
	mapping                                *bloblang.Executor
	logger                                 *service.Logger
	initStatementsFn                       func(context.Context, *streaming.SnowflakeRestClient) error
	restClient                             *streaming.SnowflakeRestClient
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
		ID:                      id,
		Name:                    name,
		DatabaseName:            o.db,
		SchemaName:              o.schema,
		TableName:               o.table,
		BuildParallelism:        o.buildParallelism,
		StrictSchemaEnforcement: o.schemaEvolutionMapping != nil,
	})
}

func (o *snowflakeStreamerOutput) Connect(ctx context.Context) error {
	if o.initStatementsFn != nil {
		if err := o.initStatementsFn(ctx, o.restClient); err != nil {
			return fmt.Errorf("unable to run initialization statement: %w", err)
		}
		// We've already executed our init statement, we don't need to do that anymore
		o.initStatementsFn = nil
	}
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
	var err error
	// We only migrate one column at a time, so tolerate up to 10 schema
	// migrations for a single batch before giving up. This protects against
	// any bugs over infinitely looping.
	for i := 0; i < 10; i++ {
		err = o.WriteBatchInternal(ctx, batch)
		if err == nil {
			return nil
		}
		migrationErr := schemaMigrationNeededError{}
		if !errors.As(err, &migrationErr) {
			break
		}
		if err := migrationErr.migrator(ctx); err != nil {
			return err
		}
	}
	return err
}

func (o *snowflakeStreamerOutput) WriteBatchInternal(ctx context.Context, batch service.MessageBatch) error {
	o.schemaMigrationMu.RLock()
	defer o.schemaMigrationMu.RUnlock()
	var channel *streaming.SnowflakeIngestionChannel
	if maybeChan := o.channelPool.Get(); maybeChan != nil {
		channel = maybeChan.(*streaming.SnowflakeIngestionChannel)
	} else {
		var err error
		if channel, err = o.openNewChannel(ctx); err != nil {
			return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
		}
	}
	o.logger.Debugf("inserting rows using channel %s", channel.Name)
	stats, err := channel.InsertRows(ctx, batch)
	if err == nil {
		o.logger.Debugf("done inserting rows using channel %s, stats: %+v", channel.Name, stats)
		o.compressedOutput.Incr(int64(stats.CompressedOutputSize))
		o.uploadTime.Timing(stats.UploadTime.Nanoseconds())
		o.buildTime.Timing(stats.BuildTime.Nanoseconds())
		o.convertTime.Timing(stats.ConvertTime.Nanoseconds())
		o.serializeTime.Timing(stats.SerializeTime.Nanoseconds())
	} else {
		// Only evolve the schema if requested.
		if o.schemaEvolutionMapping != nil {
			nullColumnErr := streaming.NonNullColumnError{}
			if errors.As(err, &nullColumnErr) {
				// put the channel back so that we can reopen it along with the rest of the channels to
				// pick up the new schema.
				o.channelPool.Put(channel)
				// Return an error so that we release our read lock and can take the write lock
				// to forcibly reopen all our channels to get a new schema.
				return schemaMigrationNeededError{
					migrator: func(ctx context.Context) error {
						return o.MigrateNotNullColumn(ctx, nullColumnErr)
					},
				}
			}
			missingColumnErr := streaming.MissingColumnError{}
			if errors.As(err, &missingColumnErr) {
				o.channelPool.Put(channel)
				return schemaMigrationNeededError{
					migrator: func(ctx context.Context) error {
						return o.MigrateMissingColumn(ctx, missingColumnErr)
					},
				}
			}
		}
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

type schemaMigrationNeededError struct {
	migrator func(ctx context.Context) error
}

func (schemaMigrationNeededError) Error() string {
	return "schema migration was required and the operation needs to be retried after the migration"
}

func (o *snowflakeStreamerOutput) MigrateMissingColumn(ctx context.Context, col streaming.MissingColumnError) error {
	o.schemaMigrationMu.Lock()
	defer o.schemaMigrationMu.Unlock()
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any{
		"name":  col.RawName(),
		"value": col.Value(),
	})
	out, err := msg.BloblangQuery(o.schemaEvolutionMapping)
	if err != nil {
		return fmt.Errorf("unable to compute new column type for %s: %w", col.ColumnName(), err)
	}
	v, err := out.AsBytes()
	if err != nil {
		return fmt.Errorf("unable to extract result from new column type mapping for %s: %w", col.ColumnName(), err)
	}
	columnType := string(v)
	if err := validateColumnType(columnType); err != nil {
		return err
	}
	o.logger.Infof("identified new schema - attempting to alter table to add column: %s %s", col.ColumnName(), columnType)
	err = o.RunSQLMigration(
		ctx,
		// This looks very scary and it *should*. This is prone to SQL injection attacks. The column name is
		// quoted according to the rules in Snowflake's documentation. This is also why we need to
		// validate the data type, so that you can't sneak an injection attack in there.
		fmt.Sprintf(`ALTER TABLE IDENTIFIER(?)
    ADD COLUMN IF NOT EXISTS %s %s
      COMMENT 'column created by schema evolution from Redpanda Connect'`,
			col.ColumnName(),
			columnType,
		),
	)
	if err != nil {
		o.logger.Warnf("unable to add new column, this maybe due to a race with another request, error: %s", err)
	}
	return o.ReopenAllChannels(ctx)
}

func (o *snowflakeStreamerOutput) MigrateNotNullColumn(ctx context.Context, col streaming.NonNullColumnError) error {
	o.schemaMigrationMu.Lock()
	defer o.schemaMigrationMu.Unlock()
	o.logger.Infof("identified new schema - attempting to alter table to remove null constraint on column: %s", col.ColumnName())
	err := o.RunSQLMigration(
		ctx,
		// This looks very scary and it *should*. This is prone to SQL injection attacks. The column name here
		// comes directly from the Snowflake API so it better not have a SQL injection :)
		fmt.Sprintf(`ALTER TABLE IDENTIFIER(?) ALTER
      %s DROP NOT NULL,
      %s COMMENT 'column altered to be nullable by schema evolution from Redpanda Connect'`,
			col.ColumnName(),
			col.ColumnName(),
		),
	)
	if err != nil {
		o.logger.Warnf("unable to mark column %s as null, this maybe due to a race with another request, error: %s", col.ColumnName(), err)
	}
	return o.ReopenAllChannels(ctx)
}

func (o *snowflakeStreamerOutput) RunSQLMigration(ctx context.Context, statement string) error {
	_, err := o.restClient.RunSQL(ctx, streaming.RunSQLRequest{
		Statement: statement,
		// Currently we set of timeout of 30 seconds so that we don't have to handle async operations
		// that need polling to wait until they finish (results are made async when execution is longer
		// than 45 seconds).
		Timeout:  30,
		Database: o.db,
		Schema:   o.schema,
		Role:     o.role,
		Bindings: map[string]streaming.BindingValue{
			"1": {Type: "TEXT", Value: o.table},
		},
	})
	return err
}

// ReopenAllChannels should be called while holding schemaMigrationMu so that
// all channels are actually processed
func (o *snowflakeStreamerOutput) ReopenAllChannels(ctx context.Context) error {
	all := []*streaming.SnowflakeIngestionChannel{}
	for {
		maybeChan := o.channelPool.Get()
		if maybeChan == nil {
			break
		}
		channel := maybeChan.(*streaming.SnowflakeIngestionChannel)
		reopened, reopenErr := o.openChannel(ctx, channel.Name, channel.ID)
		if reopenErr == nil {
			channel = reopened
		} else {
			o.logger.Warnf("unable to reopen channel %q schema migration: %v", channel.Name, reopenErr)
			// Keep the existing channel so we don't reopen channels, but instead retry later.
		}
		all = append(all, channel)
	}
	for _, c := range all {
		o.channelPool.Put(c)
	}
	return nil
}

func (o *snowflakeStreamerOutput) Close(ctx context.Context) error {
	o.restClient.Close()
	return o.client.Close()
}

// This doesn't need to fully match, but be enough to prevent SQL injection as well as
// catch common errors.
var validColumnTypeRegex = regexp.MustCompile(`^\s*(?i:NUMBER|DECIMAL|NUMERIC|INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT|FLOAT|FLOAT4|FLOAT8|DOUBLE|DOUBLE\s+PRECISION|REAL|VARCHAR|CHAR|CHARACTER|STRING|TEXT|BINARY|VARBINARY|BOOLEAN|DATE|DATETIME|TIME|TIMESTAMP|TIMESTAMP_LTZ|TIMESTAMP_NTZ|TIMESTAMP_TZ|VARIANT|OBJECT|ARRAY)\s*(?:\(\s*\d+\s*\)|\(\s*\d+\s*,\s*\d+\s*\))?\s*$`)

func validateColumnType(v string) error {
	if validColumnTypeRegex.MatchString(v) {
		return nil
	}
	return fmt.Errorf("invalid Snowflake column data type: %s", v)
}
