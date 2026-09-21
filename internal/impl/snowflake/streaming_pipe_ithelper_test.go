// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/snowflake/streaming"
	"github.com/redpanda-data/connect/v4/internal/license"
)

// ---------------------------------------------------------------------------
// Shared test harness for the snowflake_streaming_pipe integration suite
// (streaming_v2_integration_test.go, streaming_v2_maxinflight_it_test.go,
// streaming_v2_scope_probe_it_test.go).
//
// This mirrors this repo's own existing pattern for a real-Snowflake-account
// integration test (see integration_test.go's SetupConfig/SetupSnowflakeStream/
// RunSQLQuery), not the far more elaborate harness the SSv2 demo repo this
// was migrated from used: no Docker/testcontainers, no local Kafka/Redpanda
// broker, no `rpk` CLI, and no out-of-process connector binary. Rows are fed
// into the pipeline directly via service.NewStreamBuilder's
// AddBatchProducerFunc, stamping the kafka_offset/kafka_partition metadata
// the output's channel-name and offset-token templates key off of, exactly
// as v1's own integration test feeds rows via its own producer func -- a
// real Kafka broker was never actually required to exercise this output's
// behaviour, only the metadata fields it reads.
//
// # Environment variables
//
// These are named and scoped to work as a single shared credential/target
// set across every Snowflake-account-gated integration test in this
// package, present or future -- not a set private to this output. v1's own
// integration_test.go predates this and still reads its own, differently
// named env vars (SNOWFLAKE_DB instead of SNOWFLAKE_DATABASE, SNOWFLAKE_PRIVATE_KEY
// instead of SNOWFLAKE_PRIVATE_KEY_FILE, no warehouse/pipe/account_host at
// all); reworking it onto this same set is a tracked follow-up, not done
// here. Until then, running both suites means configuring both naming
// schemes side by side against the same account.
//
// All of these are read only by this test harness -- never by the
// production snowflake_streaming_pipe output itself, which takes its
// connection details from YAML config fields, not the environment. Treat
// every value here as a dedicated, disposable test credential: a role,
// user, and object namespace you're comfortable this suite creating,
// dropping, and recreating repeatedly, not anything already used by a real
// pipeline.
//
// Required:
//
//	SNOWFLAKE_ACCOUNT          Account identifier, e.g. "MYORG-MYACCOUNT". Find yours by
//	                           running `SELECT CURRENT_ORGANIZATION_NAME(), CURRENT_ACCOUNT_NAME();`
//	                           and joining the two results with a hyphen. See
//	                           https://docs.snowflake.com/en/user-guide/admin-account-identifier.
//	SNOWFLAKE_USER             Login name of the user this suite authenticates as. Run
//	                           `SELECT CURRENT_USER();` while logged in as that user, or ask
//	                           whoever administers your Snowflake account for a dedicated
//	                           service/test user's name.
//	SNOWFLAKE_ROLE             Role this suite operates as. It creates, drops, and recreates
//	                           tables and pipes in SNOWFLAKE_DATABASE/SNOWFLAKE_SCHEMA, so it
//	                           needs at least CREATE TABLE/PIPE and the ingest privileges the
//	                           production output's own `role` field documents -- see
//	                           https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges.
//	                           Run `SELECT CURRENT_ROLE();`, or `SHOW ROLES;` to see what's
//	                           available to grant SNOWFLAKE_USER.
//	SNOWFLAKE_PRIVATE_KEY_FILE Path to a PEM-encoded RSA private key file for SNOWFLAKE_USER's
//	                           key-pair authentication -- the same file shape the production
//	                           output's own `private_key_file` field takes. If this user
//	                           doesn't already have a keypair registered, generate and
//	                           register one following
//	                           https://docs.snowflake.com/en/user-guide/key-pair-auth (roughly:
//	                           `openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out
//	                           rsa_key.p8 -nocrypt`, then `ALTER USER <user> SET
//	                           RSA_PUBLIC_KEY='<derived public key>';`). Use a key dedicated to
//	                           this test suite, not one already in use for a production pipeline.
//	SNOWFLAKE_DATABASE         Database SNOWFLAKE_ROLE can create objects in. Run
//	                           `SHOW DATABASES;` to see what's available, or
//	                           `SELECT CURRENT_DATABASE();` if your session already has one set.
//	SNOWFLAKE_SCHEMA           Schema within SNOWFLAKE_DATABASE. Run
//	                           `SHOW SCHEMAS IN DATABASE <database>;`, or
//	                           `SELECT CURRENT_SCHEMA();`.
//	SNOWFLAKE_WAREHOUSE        A running (or auto-resumable) warehouse used only for this
//	                           harness's own SQL REST API calls -- fixture setup and
//	                           row-content assertions. The Snowpipe Streaming ingest path
//	                           itself never needs a warehouse; this is purely for the test
//	                           harness's own queries. Run `SHOW WAREHOUSES;`, or
//	                           `SELECT CURRENT_WAREHOUSE();`.
//	SNOWFLAKE_PIPE             Not something to look up -- a name *you choose* for a pipe this
//	                           suite creates (via `create or replace pipe`), writes through,
//	                           and leaves behind for the next run to replace. Pick something
//	                           obviously dedicated to this test suite (e.g.
//	                           "RPCN_IT_STREAMING_PIPE"), not an existing production pipe.
//	SNOWFLAKE_TABLE            Same as SNOWFLAKE_PIPE, but for the tier 1 destination table.
//	                           Tiers 2/2b derive further sibling objects from this value at
//	                           test time (appending _DIM/_ENRICHED/_FILTERED) -- no separate
//	                           env vars for those.
//
// Optional:
//
//	SNOWFLAKE_ACCOUNT_HOST     Override for accounts whose hostname doesn't follow the default
//	                           <account>.snowflakecomputing.com pattern -- most commonly
//	                           PrivateLink accounts (<account>.privatelink.snowflakecomputing.com).
//	                           Mapped straight to the production output's own `account_host`
//	                           field; see https://docs.snowflake.com/en/user-guide/admin-privatelink.
//	                           Leave unset for an ordinary account.
//	SNOWFLAKE_PRIVATE_KEY_PASS Passphrase for SNOWFLAKE_PRIVATE_KEY_FILE, only if it's
//	                           encrypted (the `-v2 aes256` form in Snowflake's key-pair-auth
//	                           guide, rather than `-nocrypt`). Leave unset for an unencrypted key.
// ---------------------------------------------------------------------------

type snowflakeITEnv struct {
	Account        string
	AccountHost    string
	User           string
	Role           string
	PrivateKeyFile string
	PrivateKeyPass string
	Database       string
	Schema         string
	Warehouse      string
	Pipe           string
	Table          string
}

// loadSnowflakeITEnv reads the SNOWFLAKE_* env vars documented above,
// skipping the test and naming what is missing if any are unset.
func loadSnowflakeITEnv(t *testing.T) *snowflakeITEnv {
	t.Helper()

	required := map[string]string{
		"SNOWFLAKE_ACCOUNT":          os.Getenv("SNOWFLAKE_ACCOUNT"),
		"SNOWFLAKE_USER":             os.Getenv("SNOWFLAKE_USER"),
		"SNOWFLAKE_ROLE":             os.Getenv("SNOWFLAKE_ROLE"),
		"SNOWFLAKE_PRIVATE_KEY_FILE": os.Getenv("SNOWFLAKE_PRIVATE_KEY_FILE"),
		"SNOWFLAKE_DATABASE":         os.Getenv("SNOWFLAKE_DATABASE"),
		"SNOWFLAKE_SCHEMA":           os.Getenv("SNOWFLAKE_SCHEMA"),
		"SNOWFLAKE_WAREHOUSE":        os.Getenv("SNOWFLAKE_WAREHOUSE"),
		"SNOWFLAKE_PIPE":             os.Getenv("SNOWFLAKE_PIPE"),
		"SNOWFLAKE_TABLE":            os.Getenv("SNOWFLAKE_TABLE"),
	}
	var missing []string
	for k, v := range required {
		if v == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Skipf("integration test skipped: missing env vars: %s", strings.Join(missing, ", "))
	}

	return &snowflakeITEnv{
		Account:        required["SNOWFLAKE_ACCOUNT"],
		AccountHost:    os.Getenv("SNOWFLAKE_ACCOUNT_HOST"),
		User:           required["SNOWFLAKE_USER"],
		Role:           required["SNOWFLAKE_ROLE"],
		PrivateKeyFile: required["SNOWFLAKE_PRIVATE_KEY_FILE"],
		PrivateKeyPass: os.Getenv("SNOWFLAKE_PRIVATE_KEY_PASS"),
		Database:       required["SNOWFLAKE_DATABASE"],
		Schema:         required["SNOWFLAKE_SCHEMA"],
		Warehouse:      required["SNOWFLAKE_WAREHOUSE"],
		Pipe:           required["SNOWFLAKE_PIPE"],
		Table:          required["SNOWFLAKE_TABLE"],
	}
}

// loadSnowflakeITPrivateKey reads and parses the private key named by
// SNOWFLAKE_PRIVATE_KEY_FILE, reusing this package's own getPrivateKey (auth.go)
// -- the same PEM/base64 parsing the production output itself uses -- rather
// than duplicating that logic in the test harness.
func loadSnowflakeITPrivateKey(t *testing.T, env *snowflakeITEnv) *rsa.PrivateKey {
	t.Helper()
	raw, err := os.ReadFile(env.PrivateKeyFile)
	require.NoError(t, err)
	pk, err := getPrivateKey(raw, env.PrivateKeyPass)
	require.NoError(t, err)
	return pk
}

// newSSv2SQLClient builds the SQL REST client used to run fixture SQL and
// assert on row contents.
func newSSv2SQLClient(t *testing.T, env *snowflakeITEnv, pk *rsa.PrivateKey) *streaming.SnowflakeRestClient {
	t.Helper()
	host := env.AccountHost
	if host == "" {
		host = strings.ToLower(env.Account) + ".snowflakecomputing.com"
	}
	client, err := streaming.NewRestClient(streaming.RestOptions{
		Account:    env.Account,
		User:       env.User,
		URL:        "https://" + host,
		PrivateKey: pk,
		Logger:     service.MockResources().Logger(),
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

// runSSv2SQL runs one statement and returns the raw response and error, so
// callers can recognise specific failures (see isTier2Unavailable).
func runSSv2SQL(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, statement string) (streaming.RunSQLResponse, error) {
	t.Helper()
	return sql.RunSQL(ctx, streaming.RunSQLRequest{
		Statement: statement,
		// RunSQL does not poll async (202) responses, so DDL must finish
		// within this.
		Timeout:   60,
		Database:  env.Database,
		Schema:    env.Schema,
		Warehouse: env.Warehouse,
		Role:      env.Role,
	})
}

// mustRunSSv2SQL is runSSv2SQL plus require.NoError, for fixture statements
// that must always succeed (as opposed to tier 2's private-preview probe,
// which treats a specific failure as an expected skip condition).
func mustRunSSv2SQL(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, statement string) streaming.RunSQLResponse {
	t.Helper()
	resp, err := runSSv2SQL(t, ctx, sql, env, statement)
	require.NoError(t, err, "statement: %s", statement)
	return resp
}

// isTier2Unavailable recognises the "private-preview feature not enabled"
// errors (003001/42501, 002098/0A000) so tier 2/2b can skip instead of fail.
// The SQL API's error body is embedded verbatim in the error message.
func isTier2Unavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "003001") || strings.Contains(msg, "42501") ||
		strings.Contains(msg, "002098") || strings.Contains(msg, "0A000")
}

// waitForSSv2RowCount polls table's row count until it reaches want or
// timeout elapses.
func waitForSSv2RowCount(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int
	for {
		resp := mustRunSSv2SQL(t, ctx, sql, env, fmt.Sprintf("select count(*) from %s", table))
		n, err := strconv.Atoi(resp.Data[0][0])
		require.NoError(t, err)
		last = n
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s to reach %d rows; last observed %d", table, want, last)
		}
		time.Sleep(2 * time.Second)
	}
}

// assertSSv2Scalar asserts that query's first column of its first row equals
// want (RunSQLResponse.Data is already [][]string, so no type coercion is
// needed on the caller's side).
func assertSSv2Scalar(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, label, query, want string) {
	t.Helper()
	resp := mustRunSSv2SQL(t, ctx, sql, env, query)
	require.NotEmpty(t, resp.Data, "%s: no rows returned for query: %s", label, query)
	require.Equal(t, want, resp.Data[0][0], "%s (query: %s)", label, query)
}

// assertSSv2Null asserts that query's first column of its first row is
// SQL NULL, which the SQL REST API v2 renders as an empty string in
// RunSQLResponse.Data.
func assertSSv2Null(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, label, query string) {
	t.Helper()
	resp := mustRunSSv2SQL(t, ctx, sql, env, query)
	require.NotEmpty(t, resp.Data, "%s: no rows returned for query: %s", label, query)
	require.Empty(t, resp.Data[0][0], "%s: expected NULL (query: %s)", label, query)
}

// Fixture SQL. Every numeric cast carries explicit scale (number(38,2)):
// a bare ::number is NUMBER(38,0) and silently rounds 12.50 to 13.

// ssv2Tier1FixtureSQL creates the tier 1 (plain projection) table and pipe.
// Gated only by ENABLE_SNOWPIPE_STREAMING_COPY_TRANSFORMATION (default TRUE,
// not a preview feature), so any failure applying these is always a hard
// failure, never a skip.
func ssv2Tier1FixtureSQL(table, pipe string) []string {
	return []string{
		fmt.Sprintf(`create or replace table %s (stake number(38,2), bet_id string)`, table),
		fmt.Sprintf(`create or replace pipe %s as copy into %s from (
			select $1:stake::number(38,2), $1:bet_id::string
			from table(data_source(type => 'STREAMING'))
		)`, pipe, table),
	}
}

// ssv2Tier2FixtureSQL creates the tier 2 dimension table, seed rows and
// enrichment pipe/table (private preview; see isTier2Unavailable). The
// PRIMARY KEY is required by the stream-static join. b-2 is absent from the
// seed rows so an unmatched row can be asserted to land with NULL.
func ssv2Tier2FixtureSQL(dimTable, enrichedTable, enrichedPipe string) []string {
	return []string{
		fmt.Sprintf(`create or replace table %s (bet_id string primary key, customer string)`, dimTable),
		fmt.Sprintf(`insert into %s values ('b-1','alice'), ('b-3','carol')`, dimTable),
		fmt.Sprintf(`create or replace table %s (stake number(38,2), bet_id string, customer string)`, enrichedTable),
		fmt.Sprintf(`create or replace pipe %s as copy into %s from (
			select s.$1:stake::number(38,2), s.$1:bet_id::string, d.customer
			from table(data_source(type => 'STREAMING')) s
			left join %s d on d.bet_id = s.$1:bet_id::string
		)`, enrichedPipe, enrichedTable, dimTable),
	}
}

// ssv2Tier2bFixtureSQL creates the tier 2b (WHERE-clause row filtering)
// table/pipe. Also gated by ENABLE_SNOWPIPE_STREAMING_WHERE_CLAUSE. The
// threshold is 2.00: a row with stake below it must never reach the
// destination table at all, not merely be excluded from a later query.
func ssv2Tier2bFixtureSQL(filteredTable, filteredPipe string) []string {
	return []string{
		fmt.Sprintf(`create or replace table %s (stake number(38,2), bet_id string)`, filteredTable),
		fmt.Sprintf(`create or replace pipe %s as copy into %s from (
			select $1:stake::number(38,2), $1:bet_id::string
			from table(data_source(type => 'STREAMING'))
			where $1:stake::number(38,2) >= 2.00
		)`, filteredPipe, filteredTable),
	}
}

// Pipeline driver: a snowflake_streaming_pipe output built via
// service.NewStreamBuilder and fed rows through AddBatchProducerFunc.

// ssv2OutputOpts customises the output config beyond pipe/max_in_flight.
// The zero value is at-least-once with the default channel; see
// defaultSSv2OutputOpts for the exactly-once shape most tests use.
type ssv2OutputOpts struct {
	// offsetToken is rendered verbatim; empty omits offset_token.
	offsetToken string
	// channelName is rendered verbatim; empty leaves channel_name unset.
	channelName       string
	tolerateRowErrors bool

	// inlinePrivateKey renders private_key instead of private_key_file.
	inlinePrivateKey string
	// privateKeyFile/privateKeyPass override env's when privateKeyFile is set.
	privateKeyFile string
	privateKeyPass string
}

// defaultSSv2OutputOpts is exactly-once via @kafka_offset on a per-partition
// channel (the shape the config linter requires for a Kafka offset token).
// All test rows use partition 0, so it is still one channel, and the
// template is deterministic so a second stream reopens the same channel.
func defaultSSv2OutputOpts() ssv2OutputOpts {
	return ssv2OutputOpts{offsetToken: "${! @kafka_offset }", channelName: "rpcn-it-p${! @kafka_partition }"}
}

// buildSSv2OutputYAML builds the snowflake_streaming_pipe output's
// component-level YAML (the form StreamBuilder.AddOutputYAML expects) for
// the given pipe.
func buildSSv2OutputYAML(env *snowflakeITEnv, pipe string, maxInFlight int, opts ssv2OutputOpts) string {
	keyFile, keyPass := env.PrivateKeyFile, env.PrivateKeyPass
	if opts.privateKeyFile != "" {
		keyFile, keyPass = opts.privateKeyFile, opts.privateKeyPass
	}
	yaml := fmt.Sprintf(`
snowflake_streaming_pipe:
  account: %q
  user: %q
  role: %q
  database: %q
  schema: %q
  pipe: %q
  max_in_flight: %d
  tolerate_row_errors: %t
`, env.Account, env.User, env.Role, env.Database, env.Schema, pipe, maxInFlight, opts.tolerateRowErrors)
	if opts.inlinePrivateKey != "" {
		yaml += fmt.Sprintf("  private_key: %q\n", opts.inlinePrivateKey)
	} else {
		yaml += fmt.Sprintf("  private_key_file: %q\n", keyFile)
	}
	if keyPass != "" {
		yaml += fmt.Sprintf("  private_key_pass: %q\n", keyPass)
	}
	if env.AccountHost != "" {
		yaml += fmt.Sprintf("  account_host: %q\n", env.AccountHost)
	}
	if opts.offsetToken != "" {
		yaml += fmt.Sprintf("  offset_token: %q\n", opts.offsetToken)
	}
	if opts.channelName != "" {
		yaml += fmt.Sprintf("  channel_name: %q\n", opts.channelName)
	}
	return yaml
}

// ssv2Row builds a message with the kafka_partition/kafka_offset metadata a
// broker would provide.
func ssv2Row(payload string, partition, offset int) *service.Message {
	m := service.NewMessage([]byte(payload))
	m.MetaSetMut("kafka_partition", partition)
	m.MetaSetMut("kafka_offset", offset)
	return m
}

// buildSSv2Stream builds a stream around a snowflake_streaming_pipe output
// (enterprise license injected) and returns its producer func and the stream.
func buildSSv2Stream(t *testing.T, env *snowflakeITEnv, pipe string, maxInFlight int, opts ssv2OutputOpts) (produce service.MessageBatchHandlerFunc, stream *service.Stream) {
	t.Helper()
	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML(`level: INFO`))
	produce, err := b.AddBatchProducerFunc()
	require.NoError(t, err)
	require.NoError(t, b.AddOutputYAML(buildSSv2OutputYAML(env, pipe, maxInFlight, opts)))
	stream, err = b.Build()
	require.NoError(t, err)
	license.InjectTestService(stream.Resources())
	return produce, stream
}

// runSSv2StreamInBackground runs stream until t's cleanup cancels it,
// failing the test if it exits with anything other than context.Canceled.
func runSSv2StreamInBackground(t *testing.T, stream *service.Stream) {
	t.Helper()
	// Not t.Context(): the stream is stopped by the cleanup below, which then
	// waits for it to exit.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := stream.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("stream run: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// sendSSv2Rows runs a stream and sends rows one at a time; each produce call
// blocks until that row's WriteBatch (including the commit wait) completes.
func sendSSv2Rows(t *testing.T, env *snowflakeITEnv, pipe string, maxInFlight int, rows []*service.Message) {
	t.Helper()
	sendSSv2RowsWithOpts(t, env, pipe, maxInFlight, defaultSSv2OutputOpts(), rows)
}

// sendSSv2RowsWithOpts is sendSSv2Rows with explicit output options.
func sendSSv2RowsWithOpts(t *testing.T, env *snowflakeITEnv, pipe string, maxInFlight int, opts ssv2OutputOpts, rows []*service.Message) {
	t.Helper()
	produce, stream := buildSSv2Stream(t, env, pipe, maxInFlight, opts)
	runSSv2StreamInBackground(t, stream)
	for _, row := range rows {
		require.NoError(t, produce(t.Context(), service.MessageBatch{row}))
	}
}

// sendSSv2Batch sends one batch through a single WriteBatch call and returns
// the produce error for callers that expect a failure.
func sendSSv2Batch(t *testing.T, env *snowflakeITEnv, pipe string, opts ssv2OutputOpts, batch service.MessageBatch) error {
	t.Helper()
	produce, stream := buildSSv2Stream(t, env, pipe, 1, opts)
	runSSv2StreamInBackground(t, stream)
	return produce(t.Context(), batch)
}
