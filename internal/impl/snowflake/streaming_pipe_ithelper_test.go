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

// loadSnowflakeITEnv reads and validates the SNOWFLAKE_* env vars documented
// above, skipping the test (and naming exactly what is missing) rather than
// ever silently passing or failing with no account configured. Callers must
// call integration.CheckSkip themselves first, per this package's
// convention.
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

// newSSv2SQLClient builds a streaming.SnowflakeRestClient (this package's own
// pre-existing SQL REST API v2 client, used by v1's integration_test.go for
// exactly the same purpose) authenticated against env for running fixture SQL
// and asserting on row contents. There is no equivalent client exposed by the
// snowflake_streaming_pipe output for tests to reach into (unlike v1's
// SnowflakeClientResourceForTesting hook), so this harness builds its own,
// independently, the same way the production output builds one for itself.
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

// runSSv2SQL runs one statement via sql and fails the test if the SQL API
// itself reports a non-success sqlState. It returns the raw response so
// callers needing to distinguish a specific failure (e.g. tier 2's
// private-preview-unavailable check) can inspect it themselves instead of
// calling this helper.
func runSSv2SQL(t *testing.T, ctx context.Context, sql *streaming.SnowflakeRestClient, env *snowflakeITEnv, statement string) (streaming.RunSQLResponse, error) {
	t.Helper()
	return sql.RunSQL(ctx, streaming.RunSQLRequest{
		Statement: statement,
		// Fixture DDL (create table/pipe) is expected to complete well within
		// this; RunSQL (rest.go) does not poll for an async (202) response,
		// so a statement that runs long enough for Snowflake to switch to
		// async would surface as a bare "non successful status code (202)"
		// error here rather than its actual result -- an existing constraint
		// of this repo's own client, not something this migration changes.
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

// isTier2Unavailable recognizes the specific "private-preview feature not
// enabled" errors (003001 / 42501, 002098 / 0A000) Snowflake returns for the
// stream-static-join/WHERE-clause Streaming Transformations features when an
// account does not have them enabled, so tier 2/2b can be skipped gracefully
// instead of failing the whole suite. The SQL API's error code/sqlState land
// in the raw response body, which streaming.SnowflakeRestClient's doPost
// embeds verbatim in the returned error's message (see rest.go), so a plain
// substring check is sufficient without parsing the body ourselves.
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

// ---------------------------------------------------------------------------
// Fixture SQL, inlined as Go string templates rather than an external
// integration/pipe_setup.sql fixture file with placeholder substitution --
// this repo's own v1 integration test embeds its fixture SQL the same way,
// as literal strings in the pipeline YAML's init_statement field. Every
// numeric cast carries explicit precision and scale (number(38,2), never
// bare number): $1:stake::number resolves to NUMBER(38,0) and silently
// rounds -- 12.50 lands as 13 -- so an unscaled cast would corrupt data
// without erroring.
// ---------------------------------------------------------------------------

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

// ssv2Tier2FixtureSQL creates the tier 2 (stream-static join enrichment)
// dimension table, its seed rows, and the enrichment pipe/table. Requires
// ENABLE_STREAMING_STATIC_JOIN and ENABLE_SNOWPIPE_STREAMING_WHERE_CLAUSE
// (private preview, default FALSE) -- see isTier2Unavailable. The dimension
// table's PRIMARY KEY is required: without it the join is rejected with
// 002364 (0A000) "Streaming stream-static JOIN requires the dimension table
// ... to have a primary key". b-2 is deliberately absent from the seed rows
// so a row referencing it exercises the "unmatched row preserved with NULL,
// not dropped" assertion.
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

// ---------------------------------------------------------------------------
// Pipeline driver: builds a snowflake_streaming_pipe output via
// service.NewStreamBuilder and feeds it rows directly through
// AddBatchProducerFunc, exactly mirroring this package's own v1
// integration_test.go (SetupSnowflakeStream/RunStreamInBackground), just
// against the pipe output's config fields.
// ---------------------------------------------------------------------------

// ssv2OutputOpts customizes buildSSv2OutputYAML/buildSSv2Stream/sendSSv2Rows
// beyond pipe/max_in_flight, for tests exercising a specific field's
// behavior rather than the default exactly-once-with-@kafka_offset shape
// every existing caller relies on. The zero value is not a usable default on
// its own -- see defaultSSv2OutputOpts, which every existing call site now
// passes explicitly to keep prior behavior unchanged.
type ssv2OutputOpts struct {
	// offsetToken is rendered as offset_token's value verbatim (so it can be
	// any interpolated expression, not just @kafka_offset). Empty omits the
	// field entirely -- the output's own at-least-once mode, not a zero
	// value passed through.
	offsetToken string
	// channelName, if non-empty, is rendered as channel_name's value
	// verbatim, overriding the output's own single-constant-channel
	// default. Empty leaves channel_name unset.
	channelName string
	// tolerateRowErrors is rendered as tolerate_row_errors's value.
	tolerateRowErrors bool

	// inlinePrivateKey, if non-empty, is rendered as the private_key field
	// (the key's own PEM/PKCS8 text, inline) instead of private_key_file --
	// for a test exercising that field specifically, since every other
	// caller authenticates via env.PrivateKeyFile. Mutually exclusive with
	// privateKeyFile, matching the output's own LintRule.
	inlinePrivateKey string
	// privateKeyFile and privateKeyPass, when privateKeyFile is non-empty,
	// override env.PrivateKeyFile/env.PrivateKeyPass -- for a test that
	// needs a *different* key file (e.g. the same registered key,
	// re-encrypted under a passphrase for this call only) without changing
	// what every other test in the package authenticates with.
	privateKeyFile string
	privateKeyPass string
}

// defaultSSv2OutputOpts matches the shape every test in this package relied
// on before ssv2OutputOpts existed: exactly-once via @kafka_offset, no
// explicit channel_name (the single-constant-channel default), row errors
// not tolerated.
func defaultSSv2OutputOpts() ssv2OutputOpts {
	return ssv2OutputOpts{offsetToken: "${! @kafka_offset }"}
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

// ssv2Row builds a message carrying the kafka_offset/kafka_partition
// metadata the output's default offset_token and channel_name templates key
// off of -- the only two fields a real Kafka broker would otherwise be
// providing here.
func ssv2Row(payload string, partition, offset int) *service.Message {
	m := service.NewMessage([]byte(payload))
	m.MetaSetMut("kafka_partition", partition)
	m.MetaSetMut("kafka_offset", offset)
	return m
}

// buildSSv2Stream builds a stream wired to a snowflake_streaming_pipe output
// for pipe and returns the batch-producer function to feed it rows plus the
// built *service.Stream, with an enterprise license already injected
// (license.InjectTestService) since this output is enterprise-gated. The
// caller is responsible for running the stream (runSSv2StreamInBackground).
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

// sendSSv2Rows builds and runs a stream for pipe, sends rows through it one
// at a time (AddBatchProducerFunc's returned function blocks until each
// batch is fully delivered downstream, i.e. until that row's own WriteBatch
// call -- including WaitUntilCommitted -- has completed), then stops the
// stream. This is the sequential send path used by the main tier 1/2/2b
// suite; streaming_v2_maxinflight_it_test.go sends concurrently instead, to
// exercise genuinely concurrent WriteBatch dispatch.
func sendSSv2Rows(t *testing.T, env *snowflakeITEnv, pipe string, maxInFlight int, rows []*service.Message) {
	t.Helper()
	sendSSv2RowsWithOpts(t, env, pipe, maxInFlight, defaultSSv2OutputOpts(), rows)
}

// sendSSv2RowsWithOpts is sendSSv2Rows with full control over the output's
// config via opts, for tests exercising offset_token/channel_name/
// tolerate_row_errors behavior rather than the default exactly-once shape.
func sendSSv2RowsWithOpts(t *testing.T, env *snowflakeITEnv, pipe string, maxInFlight int, opts ssv2OutputOpts, rows []*service.Message) {
	t.Helper()
	produce, stream := buildSSv2Stream(t, env, pipe, maxInFlight, opts)
	runSSv2StreamInBackground(t, stream)
	for _, row := range rows {
		require.NoError(t, produce(t.Context(), service.MessageBatch{row}))
	}
}

// sendSSv2Batch sends a single batch (one produce call, not one per row) via
// a stream configured with opts, for tests where multiple messages must
// travel through the same WriteBatch call -- e.g. a batch spanning more than
// one resolved channel_name, or a batch mixing a row Snowflake will reject
// with rows that should still land. Returns the error produce returned, so
// callers expecting a failure (tolerate_row_errors: false) can assert on it
// directly instead of via require.NoError.
func sendSSv2Batch(t *testing.T, env *snowflakeITEnv, pipe string, opts ssv2OutputOpts, batch service.MessageBatch) error {
	t.Helper()
	produce, stream := buildSSv2Stream(t, env, pipe, 1, opts)
	runSSv2StreamInBackground(t, stream)
	return produce(t.Context(), batch)
}
