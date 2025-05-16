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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	neturl "net/url"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/pool"
)

const (
	ssoFieldAccount                             = "account"
	ssoFieldURL                                 = "url"
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
	ssoFieldChannelName                         = "channel_name"
	ssoFieldOffsetToken                         = "offset_token"
	ssoFieldMapping                             = "mapping"
	ssoFieldBuildOpts                           = "build_options"
	ssoFieldBuildParallelismLegacy              = "build_parallelism"
	ssoFieldBuildParallelism                    = "parallelism"
	ssoFieldBuildChunkSize                      = "chunk_size"
	ssoFieldSchemaEvolution                     = "schema_evolution"
	ssoFieldSchemaEvolutionEnabled              = "enabled"
	ssoFieldSchemaEvolutionIgnoreNulls          = "ignore_nulls"
	ssoFieldSchemaEvolutionNewColumnTypeMapping = "new_column_type_mapping"
	ssoFieldSchemaEvolutionProcessors           = "processors"
	ssoFieldCommitTimeout                       = "commit_timeout"

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
			service.NewStringField(ssoFieldURL).
				Description("Override the default URL used to connect to Snowflake which is https://ORG-ACCOUNT.snowflakecomputing.com").Optional().Example("https://org-account.privatelink.snowflakecomputing.com").Advanced(),
			service.NewStringField(ssoFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user."),
			service.NewStringField(ssoFieldRole).Description("The role for the `user` field. The role must have the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] to call the Snowpipe Streaming APIs. See https://docs.snowflake.com/en/user-guide/admin-user-management#user-roles[Snowflake Documentation^] for more information about roles.").Example("ACCOUNTADMIN"),
			service.NewStringField(ssoFieldDB).Description("The Snowflake database to ingest data into.").Example("MY_DATABASE"),
			service.NewStringField(ssoFieldSchema).Description("The Snowflake schema to ingest data into.").Example("PUBLIC"),
			service.NewInterpolatedStringField(ssoFieldTable).Description("The Snowflake table to ingest data into.").Example("MY_TABLE"),
			service.NewStringField(ssoFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").Optional().Secret(), /*.LintRule(`root = if !this.re_match("(?s)^-----BEGIN [A-Z ]+-----\\n[0-9A-Za-z+/=\\n]+-----END [A-Z ]+-----\\n?$") && !this.re_match("[0-9A-Za-z+/=]") { ["field private_key must be in PEM format"] }`)*/
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
				service.NewBoolField(ssoFieldSchemaEvolutionIgnoreNulls).Description("If `true`, then new columns that are `null` are ignored and schema evolution is not triggered. If `false` then null columns trigger schema migrations in Snowflake. NOTE: unless you already know what type this column will be in advance, it's highly encouraged to ignore null values.").Default(true).Advanced(),
				service.NewBloblangField(ssoFieldSchemaEvolutionNewColumnTypeMapping).Description(`
The mapping function from Redpanda Connect type to column type in Snowflake. Overriding this can allow for customization of the datatype if there is specific information that you know about the data types in use. This mapping should result in the `+"`root`"+` variable being assigned a string with the data type for the new column in Snowflake.

        The input to this mapping is either the output of `+"`processors`"+` if specified, otherwise it is an object with the value and the name of the new column, the original message and table being written too. The metadata is unchanged from the original message that caused the schema to change. For example: `+"`"+`{"value": 42.3, "name":"new_data_field", "message": {"existing_data_field": 42, "new_data_field": "foo"}, "db": MY_DATABASE", "schema": "MY_SCHEMA", "table": "MY_TABLE"}`).Optional().Deprecated(),
				service.NewProcessorListField(ssoFieldSchemaEvolutionProcessors).Description(`
A series of processors to execute when new columns are added to the table. Specifying this can support running side effects when the schema evolves or enriching the message with additional data to guide the schema changes. For example, one could read the schema the message was produced with from the schema registry and use that to decide which type the new column in Snowflake should be.

        The input to these processors is an object with the value and the name of the new column, the original message and table being written too. The metadata is unchanged from the original message that caused the schema to change. For example: `+"`"+`{"value": 42.3, "name":"new_data_field", "message": {"existing_data_field": 42, "new_data_field": "foo"}, "db": MY_DATABASE", "schema": "MY_SCHEMA", "table": "MY_TABLE"}`+"`. The output of these series of processors should be a single message, where the contents of the message is a string indicating the column data type to use (FLOAT, VARIANT, NUMBER(38, 0), etc. An ALTER TABLE statement will then be executed on the table in Snowflake to add the column with the corresponding data type.").Optional().Advanced().Example([]map[string]any{
					{"mapping": defaultSchemaEvolutionNewColumnMapping},
				}),
			).Description(`Options to control schema evolution within the pipeline as new columns are added to the pipeline.`).Optional(),
			service.NewIntField(ssoFieldBuildParallelism).Description("The maximum amount of parallelism to use when building the output for Snowflake. The metric to watch to see if you need to change this is `snowflake_build_output_latency_ns`.").Optional().Advanced().Deprecated(),
			service.NewObjectField(ssoFieldBuildOpts,
				service.NewIntField(ssoFieldBuildParallelism).Description("The maximum amount of parallelism to use.").Default(1).LintRule(`root = if this < 1 { ["parallelism must be positive"] }`),
				service.NewIntField(ssoFieldBuildChunkSize).Description("The number of rows to chunk for parallelization.").Default(50_000).LintRule(`root = if this < 1 { ["chunk_size must be positive"] }`),
			).Advanced().Description("Options to optimize the time to build output data that is sent to Snowflake. The metric to watch to see if you need to change this is `snowflake_build_output_latency_ns`."),
			service.NewBatchPolicyField(ssoFieldBatching),
			service.NewOutputMaxInFlightField().Default(4),
			service.NewStringField(ssoFieldChannelPrefix).
				Description(`The prefix to use when creating a channel name.
Duplicate channel names will result in errors and prevent multiple instances of Redpanda Connect from writing at the same time.
By default if neither `+"`"+ssoFieldChannelPrefix+"` or `"+ssoFieldChannelName+` is specified then the output will create a channel name that is based on the table FQN so there will only be a single stream per table.

At most `+"`max_in_flight`"+` channels will be opened.

This option is mutually exclusive with `+"`"+ssoFieldChannelName+"`"+`.

NOTE: There is a limit of 10,000 streams per table - if using more than 10k streams please reach out to Snowflake support.`).
				Optional().
				Advanced().
				Example(`channel-${HOST}`),
			service.NewInterpolatedStringField(ssoFieldChannelName).
				Description(`The channel name to use.
Duplicate channel names will result in errors and prevent multiple instances of Redpanda Connect from writing at the same time.
Note that batches are assumed to all contain messages for the same channel, so this interpolation is only executed on the first
message in each batch. It's recommended to batch at the input level to ensure that batches contain messages for the same channel
if using an input that is partitioned (such as an Apache Kafka topic).

This option is mutually exclusive with `+"`"+ssoFieldChannelPrefix+"`"+`.

NOTE: There is a limit of 10,000 streams per table - if using more than 10k streams please reach out to Snowflake support.`).
				Optional().
				Advanced().
				Examples(`partition-${!@kafka_partition}`),
			service.NewInterpolatedStringField(ssoFieldOffsetToken).
				Description(`The offset token to use for exactly once delivery of data in the pipeline. When data is sent on a channel, each message in a batch's offset token
is compared to the latest token for a channel. If the offset token is lexicographically less than the latest in the channel, it's assumed the message is a duplicate and
is dropped. This means it is *very important* to have ordered delivery to the output, any out of order messages to the output will be seen as duplicates and dropped.
Specifically this means that retried messages could be seen as duplicates if later messages have succeeded in the meantime, so in most circumstances a dead letter queue
output should be employed for failed messages.

NOTE: It's assumed that messages within a batch are in increasing order by offset token, additionally if you're using a numeric value as an offset token, make sure to pad
      the value so that it's lexicographically ordered in its string representation, since offset tokens are compared in string form.

For more information about offset tokens, see https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#offset-tokens[^Snowflake Documentation]`).
				Optional().
				Advanced().
				Examples(`offset-${!"%016X".format(@kafka_offset)}`, `postgres-${!@lsn}`),
			service.NewDurationField(ssoFieldCommitTimeout).
				Description(`The max duration to wait until the data has been asynchronously committed to Snowflake.`).
				Default("60s").
				Advanced().
				Example("10s").
				Example("10m"),
		).
		LintRule(`root = match {
  this.exists("private_key") && this.exists("private_key_file") => [ "both `+"`private_key`"+` and `+"`private_key_file`"+` can't be set simultaneously" ],
}`).
		LintRule(`root = match {
  this.exists("channel_prefix") && this.exists("channel_name") => [ "both `+"`channel_prefix`"+` and `+"`channel_name`"+` can't be set simultaneously" ],
}`).
		Example(
			"Exactly once CDC into Snowflake",
			`How to send data from a PostgreSQL table into Snowflake exactly once using Postgres Logical Replication.

NOTE: If attempting to do exactly-once it's important that rows are delivered in order to the output. Be sure to read the documentation for offset_token first.
Removing the offset_token is a safer option that will instruct Redpanda Connect to use its default at-least-once delivery model instead.`,
			`
input:
  postgres_cdc:
    dsn: postgres://foouser:foopass@localhost:5432/foodb
    schema: "public"
    slot_name: "my_repl_slot"
    tables: ["my_pg_table"]
    # We want very large batches - each batch will be sent to Snowflake individually
    # so to optimize query performance we want as big of files as we have memory for
    batching:
      count: 50000
      period: 45s
    # Prevent multiple batches from being in flight at once, so that we never send
    # a batch while another batch is being retried, this is important to ensure that
    # the Snowflake Snowpipe Streaming channel does not see older data - as it will
    # assume that the older data is already committed.
    checkpoint_limit: 1
output:
  snowflake_streaming:
    # We use the log sequence number in the WAL from Postgres to ensure we
    # only upload data exactly once, these are already lexicographically
    # ordered.
    offset_token: "${!@lsn}"
    # Since we're sending a single ordered log, we can only send one thing
    # at a time to ensure that we're properly incrementing our offset_token
    # and only using a single channel at a time.
    max_in_flight: 1
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: ACCOUNTADMIN
    database: "MYDATABASE"
    schema: "PUBLIC"
    table: "MY_PG_TABLE"
    private_key_file: "my/private/key.p8"
`).
		Example(
			"Ingesting data exactly once from Redpanda",
			`How to ingest data from Redpanda with consumer groups, decode the schema using the schema registry, then write the corresponding data into Snowflake exactly once.

NOTE: If attempting to do exactly-once its important that records are delivered in order to the output and correctly partitioned. Be sure to read the documentation for 
channel_name and offset_token first. Removing the offset_token is a safer option that will instruct Redpanda Connect to use its default at-least-once delivery model instead.`,
			`
input:
  redpanda_common:
    topics: ["my_topic_going_to_snow"]
    consumer_group: "redpanda_connect_to_snowflake"
    # We want very large batches - each batch will be sent to Snowflake individually
    # so to optimize query performance we want as big of files as we have memory for
    fetch_max_bytes: 100MiB
    fetch_min_bytes: 50MiB
    partition_buffer_bytes: 100MiB
pipeline:
  processors:
    - schema_registry_decode:
        url: "redpanda.example.com:8081"
        basic_auth:
          enabled: true
          username: MY_USER_NAME
          password: "${TODO}"
output:
  fallback:
    - snowflake_streaming:
        # To ensure that we write an ordered stream each partition in kafka gets its own
        # channel.
        channel_name: "partition-${!@kafka_partition}"
        # Ensure that our offsets are lexicographically sorted in string form by padding with
        # leading zeros
        offset_token: offset-${!"%016X".format(@kafka_offset)}
        account: "MYSNOW-ACCOUNT"
        user: MYUSER
        role: ACCOUNTADMIN
        database: "MYDATABASE"
        schema: "PUBLIC"
        table: "MYTABLE"
        private_key_file: "my/private/key.p8"
        schema_evolution:
          enabled: true
    # In order to prevent delivery orders from messing with the order of delivered records
    # it's important that failures are immediately sent to a dead letter queue and not retried
    # to Snowflake. See the ordering documentation for the "redpanda" input for more details.
    - retry:
        output:
          redpanda_common:
            topic: "dead_letter_queue"
`,
		).
		Example(
			"HTTP Server to push data to Snowflake",
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
    # By default there is only a single channel per output table allowed
    # if we want to have multiple Redpanda Connect streams writing data
    # then we need a unique channel prefix per stream. We'll use the host
    # name to get unique prefixes in this example.
    channel_prefix: "snowflake-channel-for-${HOST}"
    schema_evolution:
      enabled: true
`,
		)
}

func init() {
	service.MustRegisterBatchOutput(
		"snowflake_streaming",
		snowflakeStreamingOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if err = license.CheckRunningEnterprise(mgr); err != nil {
				return
			}

			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(ssoFieldBatching); err != nil {
				return
			}
			output, err = newSnowflakeStreamer(conf, mgr)
			return
		})

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
	var url string
	if conf.Contains(ssoFieldURL) {
		url, err = conf.FieldString(ssoFieldAccount)
		if err != nil {
			return nil, err
		}
		_, err := neturl.Parse(url)
		if err != nil {
			return nil, fmt.Errorf("invalid url: %w", err)
		}
	} else {
		url = fmt.Sprintf("https://%s.snowflakecomputing.com", account)
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
	dynamicTable, err := conf.FieldInterpolatedString(ssoFieldTable)
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
	schemaEvolutionMode := streaming.SchemaModeIgnoreExtra
	var schemaEvolutionProcessors []*service.OwnedProcessor
	var schemaEvolutionMapping *bloblang.Executor
	if conf.Contains(ssoFieldSchemaEvolution, ssoFieldSchemaEvolutionEnabled) {
		seConf := conf.Namespace(ssoFieldSchemaEvolution)
		schemaEvolutionEnabled, err := seConf.FieldBool(ssoFieldSchemaEvolutionEnabled)
		if err != nil {
			return nil, err
		}
		ignoreNulls, err := seConf.FieldBool(ssoFieldSchemaEvolutionIgnoreNulls)
		if err != nil {
			return nil, err
		}
		if schemaEvolutionEnabled {
			schemaEvolutionMode = streaming.SchemaModeStrict
			if !ignoreNulls {
				schemaEvolutionMode = streaming.SchemaModeStrictWithNulls
			}
		}
		if seConf.Contains(ssoFieldSchemaEvolutionProcessors) {
			schemaEvolutionProcessors, err = seConf.FieldProcessorList(ssoFieldSchemaEvolutionProcessors)
			if err != nil {
				return nil, err
			}
		}
		if seConf.Contains(ssoFieldSchemaEvolutionNewColumnTypeMapping) {
			schemaEvolutionMapping, err = seConf.FieldBloblang(ssoFieldSchemaEvolutionNewColumnTypeMapping)
			if err != nil {
				return nil, err
			}
		}
	}

	var buildOpts streaming.BuildOptions
	buildOpts.Parallelism, err = conf.FieldInt(ssoFieldBuildOpts, ssoFieldBuildParallelism)
	if err != nil {
		return nil, err
	}
	buildOpts.ChunkSize, err = conf.FieldInt(ssoFieldBuildOpts, ssoFieldBuildChunkSize)
	if err != nil {
		return nil, err
	}
	if conf.Contains(ssoFieldBuildParallelismLegacy) {
		buildOpts.Parallelism, err = conf.FieldInt(ssoFieldBuildParallelismLegacy)
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
	}

	var channelName *service.InterpolatedString
	if conf.Contains(ssoFieldChannelName) {
		channelName, err = conf.FieldInterpolatedString(ssoFieldChannelName)
		if err != nil {
			return nil, err
		}
	}

	if (channelName != nil) && (len(channelPrefix) > 0) {
		return nil, fmt.Errorf("only one of `%s` or `%s` can be specified", ssoFieldChannelName, ssoFieldChannelPrefix)
	}

	var offsetToken *service.InterpolatedString
	if conf.Contains(ssoFieldOffsetToken) {
		offsetToken, err = conf.FieldInterpolatedString(ssoFieldOffsetToken)
		if err != nil {
			return nil, err
		}
	}

	maxInFlight, err := conf.FieldMaxInFlight()
	if err != nil {
		return nil, err
	}

	commitTimeout, err := conf.FieldDuration(ssoFieldCommitTimeout)
	if err != nil {
		return nil, err
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
				// Currently we set a of timeout of 30 seconds so that we don't have to handle async operations
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
	restClient, err := streaming.NewRestClient(streaming.RestOptions{
		Account:    account,
		URL:        url,
		User:       user,
		Version:    mgr.EngineVersion(),
		PrivateKey: rsaKey,
		Logger:     mgr.Logger(),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create rest API client: %w", err)
	}
	client, err := streaming.NewSnowflakeServiceClient(
		context.Background(),
		streaming.ClientOptions{
			Account:        account,
			URL:            url,
			User:           user,
			Role:           role,
			PrivateKey:     rsaKey,
			Logger:         mgr.Logger(),
			ConnectVersion: mgr.EngineVersion(),
		})
	if err != nil {
		return nil, err
	}

	mgr.SetGeneric(SnowflakeClientResourceForTesting, restClient)
	makeImpl := func(table string) (*snowpipeSchemaEvolver, service.BatchOutput) {
		var schemaEvolver *snowpipeSchemaEvolver
		if schemaEvolutionMode != streaming.SchemaModeIgnoreExtra {
			schemaEvolver = &snowpipeSchemaEvolver{
				mode:                   schemaEvolutionMode,
				schemaEvolutionMapping: schemaEvolutionMapping,
				pipeline:               schemaEvolutionProcessors,
				restClient:             restClient,
				logger:                 mgr.Logger(),
				db:                     db,
				schema:                 schema,
				table:                  table,
				role:                   role,
			}
		}
		var impl service.BatchOutput
		if channelName != nil {
			indexed := &snowpipeIndexedOutput{
				channelName:   channelName,
				client:        client,
				db:            db,
				schema:        schema,
				table:         table,
				role:          role,
				logger:        mgr.Logger(),
				metrics:       newSnowpipeMetrics(mgr.Metrics()),
				buildOpts:     buildOpts,
				offsetToken:   offsetToken,
				schemaMode:    schemaEvolutionMode,
				commitTimeout: commitTimeout,
			}
			indexed.channelPool = pool.NewIndexed(func(ctx context.Context, name string) (*streaming.SnowflakeIngestionChannel, error) {
				hash := sha256.Sum256([]byte(name))
				id := binary.BigEndian.Uint16(hash[:])
				return indexed.openChannel(ctx, name, int16(id))
			})
			impl = indexed
		} else {
			if channelPrefix == "" {
				// There is a limit of 10k channels, so we can't dynamically create them.
				// The only other good default is to create one and only allow a single
				// stream to write to a single table.
				channelPrefix = fmt.Sprintf("Redpanda_Connect_%s.%s.%s", db, schema, table)
			}
			pooled := &snowpipePooledOutput{
				channelPrefix: channelPrefix,
				client:        client,
				db:            db,
				schema:        schema,
				table:         table,
				role:          role,
				logger:        mgr.Logger(),
				metrics:       newSnowpipeMetrics(mgr.Metrics()),
				buildOpts:     buildOpts,
				offsetToken:   offsetToken,
				schemaMode:    schemaEvolutionMode,
				commitTimeout: commitTimeout,
			}
			pooled.channelPool = pool.NewCapped(maxInFlight, func(ctx context.Context, id int) (*streaming.SnowflakeIngestionChannel, error) {
				name := fmt.Sprintf("%s_%d", pooled.channelPrefix, id)
				return pooled.openChannel(ctx, name, int16(id))
			})
			impl = pooled
		}
		return schemaEvolver, impl
	}

	if table, ok := dynamicTable.Static(); ok {
		schemaEvolver, impl := makeImpl(table)
		return &snowpipeStreamingOutput{
			initStatementsFn: initStatementsFn,
			client:           client,
			restClient:       restClient,
			mapping:          mapping,
			logger:           mgr.Logger(),
			schemaEvolver:    schemaEvolver,

			impl: impl,
		}, nil
	} else {
		return &dynamicSnowpipeStreamingOutput{
			table: dynamicTable,
			byTable: pool.NewIndexed(func(ctx context.Context, table string) (service.BatchOutput, error) {
				schemaEvolver, impl := makeImpl(table)
				o := &snowpipeStreamingOutput{
					initStatementsFn: nil,
					client:           nil,
					restClient:       nil,
					mapping:          mapping,
					logger:           mgr.Logger(),
					schemaEvolver:    schemaEvolver,

					impl: impl,
				}
				if err := o.Connect(ctx); err != nil {
					return nil, err
				}
				return o, nil
			}),
			initStatementsFn: initStatementsFn,
			client:           client,
			restClient:       restClient,
		}, nil
	}
}

type snowflakeClientForTesting string

// SnowflakeClientResourceForTesting is a key that can be used to access the REST client for the snowflake output
// which can remove boilerplate from tests to setup a new REST client.
const SnowflakeClientResourceForTesting snowflakeClientForTesting = "SnowflakeClientResourceForTesting"

type dynamicSnowpipeStreamingOutput struct {
	table   *service.InterpolatedString
	byTable pool.Indexed[service.BatchOutput]

	initStatementsFn func(context.Context, *streaming.SnowflakeRestClient) error
	client           *streaming.SnowflakeServiceClient
	restClient       *streaming.SnowflakeRestClient
}

func (o *dynamicSnowpipeStreamingOutput) Connect(ctx context.Context) error {
	if o.initStatementsFn != nil {
		if err := o.initStatementsFn(ctx, o.restClient); err != nil {
			return fmt.Errorf("unable to run initialization statement: %w", err)
		}
		// We've already executed our init statement, we don't need to do that anymore
		o.initStatementsFn = nil
	}
	return nil
}

func (o *dynamicSnowpipeStreamingOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	executor := batch.InterpolationExecutor(o.table)
	tableBatches := map[string]service.MessageBatch{}
	for i, msg := range batch {
		table, err := executor.TryString(i)
		if err != nil {
			return fmt.Errorf("unable to interpolate `%s`: %w", ssoFieldTable, err)
		}
		tableBatches[table] = append(tableBatches[table], msg)
	}
	for table, batch := range tableBatches {
		output, err := o.byTable.Acquire(ctx, table)
		if err != nil {
			return err
		}
		// Immediately release, these are thread safe, so we can let other
		// threads modify them while we have a reference.
		o.byTable.Release(table, output)
		if err := output.WriteBatch(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (o *dynamicSnowpipeStreamingOutput) Close(ctx context.Context) error {
	for _, key := range o.byTable.Keys() {
		out, err := o.byTable.Acquire(ctx, key)
		if err != nil {
			return err
		}
		o.byTable.Release(key, out)
		if err := out.Close(ctx); err != nil {
			return err
		}
	}
	o.byTable.Reset()
	o.client.Close()
	o.restClient.Close()
	return nil
}

type snowpipeStreamingOutput struct {
	initStatementsFn func(context.Context, *streaming.SnowflakeRestClient) error
	client           *streaming.SnowflakeServiceClient
	restClient       *streaming.SnowflakeRestClient
	mapping          *bloblang.Executor
	logger           *service.Logger
	schemaEvolver    *snowpipeSchemaEvolver

	mu sync.RWMutex

	impl service.BatchOutput
}

func (o *snowpipeStreamingOutput) Connect(ctx context.Context) error {
	if o.initStatementsFn != nil {
		if err := o.initStatementsFn(ctx, o.restClient); err != nil {
			return fmt.Errorf("unable to run initialization statement: %w", err)
		}
		// We've already executed our init statement, we don't need to do that anymore
		o.initStatementsFn = nil
	}
	return o.impl.Connect(ctx)
}

func (o *snowpipeStreamingOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if len(batch) == 0 {
		return nil
	}
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
		o.mu.RLock()
		err = o.impl.WriteBatch(ctx, batch)
		o.mu.RUnlock()
		if err == nil {
			return nil
		}
		if o.schemaEvolver == nil {
			return err
		}
		if streaming.IsTableNotExistsError(err) {
			o.mu.Lock()
			err := o.createTable(ctx, batch)
			o.mu.Unlock()
			if err != nil {
				return err
			}
			continue // If creating the table succeeded, retry
		}
		// There are a class of errors that can happen under normal operation and we want to transparently
		// retry them after reopening the channel. However we only do this kind of retry once.
		if i == 0 {
			var ingestionErr *streaming.IngestionFailedError
			if errors.As(err, &ingestionErr) && ingestionErr.CanRetry() {
				continue
			}
			if errors.Is(err, &streaming.NotCommittedError{}) && i == 0 {
				// If we didn't successfully commit, then it's possible something
				// like the schema evolved before the commit went through on the
				// snowflake side
				continue
			}
		}
		var needsMigrationErr *schemaMigrationNeededError
		if !errors.As(err, &needsMigrationErr) {
			return err
		}
		o.mu.Lock()
		migrateErr := o.runMigration(ctx, needsMigrationErr)
		o.mu.Unlock()
		if migrateErr != nil {
			return migrateErr
		}
	}
	return err
}

func (o *snowpipeStreamingOutput) createTable(ctx context.Context, batch service.MessageBatch) error {
	if err := o.schemaEvolver.CreateOutputTable(ctx, batch); err != nil {
		return err
	}
	if err := o.impl.Connect(ctx); err != nil {
		return err
	}
	return nil
}

// runMigration requires the migration lock being held.
func (o *snowpipeStreamingOutput) runMigration(ctx context.Context, needsMigrationErr *schemaMigrationNeededError) error {
	if err := needsMigrationErr.runMigration(ctx, o.schemaEvolver); err != nil {
		return err
	}
	// After a migration we need to reopen all our channels
	// so close and reopen our impl
	if err := o.impl.Close(ctx); err != nil {
		return err
	}
	if err := o.impl.Connect(ctx); err != nil {
		return err
	}
	return nil
}

func (o *snowpipeStreamingOutput) Close(ctx context.Context) error {
	if err := o.impl.Close(ctx); err != nil {
		return err
	}
	if o.client != nil {
		o.client.Close()
	}
	if o.restClient != nil {
		o.restClient.Close()
	}
	return nil
}

type snowpipePooledOutput struct {
	client        *streaming.SnowflakeServiceClient
	channelPool   pool.Capped[*streaming.SnowflakeIngestionChannel]
	metrics       *snowpipeMetrics
	buildOpts     streaming.BuildOptions
	commitTimeout time.Duration

	channelPrefix, db, schema, table, role string
	offsetToken                            *service.InterpolatedString
	logger                                 *service.Logger
	schemaMode                             streaming.SchemaMode
}

func (o *snowpipePooledOutput) openChannel(ctx context.Context, name string, id int16) (*streaming.SnowflakeIngestionChannel, error) {
	o.logger.Debugf("opening snowflake streaming channel for table `%s.%s.%s`: %s", o.db, o.schema, o.table, name)
	return o.client.OpenChannel(ctx, streaming.ChannelOptions{
		ID:           id,
		Name:         name,
		DatabaseName: o.db,
		SchemaName:   o.schema,
		TableName:    o.table,
		BuildOptions: o.buildOpts,
		SchemaMode:   o.schemaMode,
	})
}

func (o *snowpipePooledOutput) Connect(ctx context.Context) error {
	return nil
}

func (o *snowpipePooledOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	channel, err := o.channelPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
	}
	var offsets *streaming.OffsetTokenRange
	if o.offsetToken != nil {
		batch, offsets, err = preprocessForExactlyOnce(channel, o.offsetToken, batch)
		if err != nil || len(batch) == 0 {
			o.channelPool.Release(channel)
			return err
		}
		o.logger.Debugf("inserting rows using channel %s at offsets: %+v", channel.Name, *offsets)
	} else {
		o.logger.Debugf("inserting rows using channel %s", channel.Name)
	}
	stats, err := channel.InsertRows(ctx, batch, offsets)
	if err != nil {
		// Only evolve the schema if requested.
		var schemaErr *schemaMigrationNeededError
		if o.schemaMode != streaming.SchemaModeIgnoreExtra {
			var ok bool
			schemaErr, ok = asSchemaMigrationError(err)
			if !ok {
				schemaErr = nil
			}
			// Always attempt to reopen the channel when there are schema errors as the user could
			// have migrated the schema in their pipeline and invalidated the channel. Worst case
			// we reopen the channel twice, which is fine as we assume schema changes are rare.
		}
		reopened, reopenErr := o.openChannel(ctx, channel.Name, channel.ID)
		if reopenErr == nil {
			o.channelPool.Release(reopened)
		} else {
			o.logger.Warnf("unable to reopen channel %q after failure: %v", channel.Name, reopenErr)
			// Keep around the same channel so retry opening later
			o.channelPool.Release(channel)
		}
		if schemaErr != nil {
			return schemaErr
		}
		return wrapInsertError(err)
	}
	o.logger.Debugf("done inserting %d rows using channel %s, stats: %+v", len(batch), channel.Name, stats)
	commitStart := time.Now()
	polls, err := channel.WaitUntilCommitted(ctx, o.commitTimeout)
	if err != nil {
		reopened, reopenErr := o.openChannel(ctx, channel.Name, channel.ID)
		if reopenErr == nil {
			o.channelPool.Release(reopened)
		} else {
			o.logger.Warnf("unable to reopen channel %q after failure: %v", channel.Name, reopenErr)
			// Keep around the same channel so retry opening later
			o.channelPool.Release(channel)
		}
		return err
	}
	commitDuration := time.Since(commitStart)
	o.logger.Debugf("batch of %d rows committed using channel %s after %d polls in %s", len(batch), channel.Name, polls, commitDuration)
	o.metrics.Report(stats, commitDuration)
	o.channelPool.Release(channel)
	return nil
}

func (o *snowpipePooledOutput) Close(ctx context.Context) error {
	o.channelPool.Reset()
	return nil
}

type snowpipeIndexedOutput struct {
	client        *streaming.SnowflakeServiceClient
	channelPool   pool.Indexed[*streaming.SnowflakeIngestionChannel]
	metrics       *snowpipeMetrics
	buildOpts     streaming.BuildOptions
	commitTimeout time.Duration

	db, schema, table, role  string
	offsetToken, channelName *service.InterpolatedString
	logger                   *service.Logger
	schemaMode               streaming.SchemaMode
}

func (o *snowpipeIndexedOutput) openChannel(ctx context.Context, name string, id int16) (*streaming.SnowflakeIngestionChannel, error) {
	o.logger.Debugf("opening snowflake streaming channel for table `%s.%s.%s`: %s", o.db, o.schema, o.table, name)
	return o.client.OpenChannel(ctx, streaming.ChannelOptions{
		ID:           id,
		Name:         name,
		DatabaseName: o.db,
		SchemaName:   o.schema,
		TableName:    o.table,
		BuildOptions: o.buildOpts,
		SchemaMode:   o.schemaMode,
	})
}

func (o *snowpipeIndexedOutput) Connect(ctx context.Context) error {
	return nil
}

func (o *snowpipeIndexedOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	channelName, err := batch.TryInterpolatedString(0, o.channelName)
	if err != nil {
		return fmt.Errorf("error executing %s: %w", ssoFieldChannelName, err)
	}
	channel, err := o.channelPool.Acquire(ctx, channelName)
	if err != nil {
		return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
	}
	var offsets *streaming.OffsetTokenRange
	if o.offsetToken != nil {
		batch, offsets, err = preprocessForExactlyOnce(channel, o.offsetToken, batch)
		if err != nil || len(batch) == 0 {
			o.channelPool.Release(channel.Name, channel)
			return err
		}
		o.logger.Debugf("inserting rows using channel %s at offsets: %+v", channel.Name, *offsets)
	} else {
		o.logger.Debugf("inserting rows using channel %s", channel.Name)
	}
	stats, err := channel.InsertRows(ctx, batch, offsets)
	if err != nil {
		// Only evolve the schema if requested.
		var schemaErr *schemaMigrationNeededError
		if o.schemaMode != streaming.SchemaModeIgnoreExtra {
			var ok bool
			schemaErr, ok = asSchemaMigrationError(err)
			if !ok {
				schemaErr = nil
			}
			// Always attempt to reopen the channel when there are schema errors as the user could
			// have migrated the schema in their pipeline and invalidated the channel. Worst case
			// we reopen the channel twice, which is fine as we assume schema changes are rare.
		}
		reopened, reopenErr := o.openChannel(ctx, channel.Name, channel.ID)
		if reopenErr == nil {
			o.channelPool.Release(channel.Name, reopened)
		} else {
			o.logger.Warnf("unable to reopen channel %q after failure: %v", channel.Name, reopenErr)
			// Keep around the same channel so retry opening later
			o.channelPool.Release(channel.Name, channel)
		}
		if schemaErr != nil {
			return schemaErr
		}
		return wrapInsertError(err)
	}
	o.logger.Debugf("done inserting %d rows using channel %s, stats: %+v", len(batch), channel.Name, stats)
	commitStart := time.Now()
	polls, err := channel.WaitUntilCommitted(ctx, o.commitTimeout)
	if err != nil {
		reopened, reopenErr := o.openChannel(ctx, channel.Name, channel.ID)
		if reopenErr == nil {
			o.channelPool.Release(channel.Name, reopened)
		} else {
			o.logger.Warnf("unable to reopen channel %q after failure: %v", channel.Name, reopenErr)
			// Keep around the same channel so retry opening later
			o.channelPool.Release(channel.Name, channel)
		}
		return err
	}
	commitDuration := time.Since(commitStart)
	o.logger.Debugf("batch of %d rows committed using channel %s after %d polls in %s", len(batch), channel.Name, polls, commitDuration)
	o.metrics.Report(stats, commitDuration)
	o.channelPool.Release(channel.Name, channel)
	return nil
}

func (o *snowpipeIndexedOutput) Close(ctx context.Context) error {
	o.channelPool.Reset()
	return nil
}

func preprocessForExactlyOnce(
	channel *streaming.SnowflakeIngestionChannel,
	offsetTokenMapping *service.InterpolatedString,
	batch service.MessageBatch,
) (service.MessageBatch, *streaming.OffsetTokenRange, error) {
	latest := channel.LatestOffsetToken()
	exec := batch.InterpolationExecutor(offsetTokenMapping)
	firstRawToken, err := exec.TryString(0)
	if err != nil {
		return nil, nil, err
	}
	lastRawToken, err := exec.TryString(len(batch) - 1)
	if err != nil {
		return nil, nil, err
	}
	// Common case, all data is new
	if latest == nil || firstRawToken > string(*latest) {
		return batch, &streaming.OffsetTokenRange{Start: streaming.OffsetToken(firstRawToken), End: streaming.OffsetToken(lastRawToken)}, nil
	}
	// We need to filter out data that is too old.
	filteredBatch := make(service.MessageBatch, 0, len(batch))
	var rawToken string
	for i := range batch {
		rawToken, err = exec.TryString(i)
		if err != nil {
			return nil, nil, err
		}
		if rawToken <= string(*latest) {
			continue
		}
		filteredBatch = append(filteredBatch, batch[i])
	}
	if len(filteredBatch) == 0 {
		return filteredBatch, nil, nil
	}
	// This is a lazy way to compute the bounds, but filtering should be a rare operation.
	return preprocessForExactlyOnce(channel, offsetTokenMapping, filteredBatch)
}

func wrapInsertError(err error) error {
	if errors.Is(err, &streaming.InvalidTimestampFormatError{}) {
		return fmt.Errorf("%w; if a custom format is required use a `%s` and bloblang functions `ts_parse` or `ts_strftime` to convert a custom format into a timestamp", err, ssoFieldMapping)
	}
	return err
}
