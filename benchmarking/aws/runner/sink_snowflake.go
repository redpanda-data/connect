// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/snowflakedb/gosnowflake/v2"
)

// The snowflake sink writes to a per-run schema, BENCH_<SESSION>, in the
// bench Snowflake account, with the bench warehouse resized to the
// scenario's reference size for the run.
//
// Key handling: the orchestrator reads the credentials secret with its own
// AWS credentials and keeps the parsed key in memory for its own SQL. Connect
// on the runner host never receives the key from the orchestrator: the bench
// script fetches the same secret on the host (the host role may read exactly
// that secret), writes the PEM to a 0600 file, and the output reads it via
// private_key_file. The key is never in a placeholder, a rendered config, an
// SSM command, a Plan or a log line.
const (
	// snowflakeSecretID is the Secrets Manager secret holding the bench
	// account's credentials, as JSON {account, user, private_key, database,
	// warehouse, role}.
	snowflakeSecretID = "redpanda-connect-bench/snowflake"
	snowflakeKeyDir   = "/opt/bench/secrets"
	snowflakeKeyFile  = snowflakeKeyDir + "/snowflake_key.p8"
	// snowflakeTable is the table the output writes to in the run's schema.
	snowflakeTable = "T"
	// snowflakeStage is the internal stage a snowflake_put output uploads to.
	snowflakeStage = "S"
	// snowflakeBaseSize is the warehouse size outside a run.
	snowflakeBaseSize = "XSMALL"
	// Snowpipe Streaming rows become visible to queries with a delay, so a
	// count is re-read every snowflakePollInterval until two reads agree or
	// snowflakePollMax has passed.
	snowflakePollInterval = 10 * time.Second
	snowflakePollMax      = 90 * time.Second
)

// SnowflakeExec runs SQL against the bench account. The gosnowflake adapter
// satisfies it; tests fake it.
type SnowflakeExec interface {
	Exec(ctx context.Context, query string) error
	// QueryInt runs a query returning one integer. ErrSnowflakeNoObject
	// when the queried object does not exist.
	QueryInt(ctx context.Context, query string) (int64, error)
}

// ErrSnowflakeNoObject is QueryInt's error for a table that does not exist
// (Snowflake error 002003).
var ErrSnowflakeNoObject = errors.New("snowflake object does not exist")

// SnowflakeTarget is the non-secret part of the bench account credentials.
type SnowflakeTarget struct {
	Account, User, Role, Database, Warehouse string
}

var (
	snowflakeIdentRe = regexp.MustCompile(`^[A-Z0-9_]+$`)
	// snowflakeNameRe is what the account, user and role from the secret
	// may hold (account identifiers carry '-' and '.').
	snowflakeNameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

// validSnowflakeIdent admits only unquoted upper-case identifiers, so a name
// interpolated into SQL cannot change the statement.
func validSnowflakeIdent(s string) error {
	if !snowflakeIdentRe.MatchString(s) {
		return fmt.Errorf("snowflake identifier %q must match %s", s, snowflakeIdentRe)
	}
	return nil
}

// snowflakeSchema is the run's schema: BENCH_<SESSION>.
func snowflakeSchema(session string) string {
	return "BENCH_" + strings.ToUpper(scratchName(session))
}

type snowflakeSink struct {
	exec   SnowflakeExec
	target SnowflakeTarget
	region string
	size   string
	// put is set for a snowflake_put output: it uploads to a stage and
	// loads no rows (the customer's snowpipe is not kept), so there is
	// nothing to count.
	put bool
	// schema is the run's schema, from SinkDeps.SessionID; Count reads it.
	schema string
	// seenRows is set once a count has read rows: from then on the table
	// must exist.
	seenRows bool
	now      func() time.Time
	sleep    func(ctx context.Context, d time.Duration) error
}

func newSnowflakeSink(spec SinkSpec, deps SinkDeps) (*snowflakeSink, error) {
	if deps.Snowflake == nil {
		return nil, errors.New("snowflake sink: no snowflake client")
	}
	t := deps.SnowflakeTarget
	t.Database, t.Warehouse = strings.ToUpper(t.Database), strings.ToUpper(t.Warehouse)
	for _, id := range []string{t.Database, t.Warehouse} {
		if err := validSnowflakeIdent(id); err != nil {
			return nil, fmt.Errorf("snowflake sink: %w", err)
		}
	}
	size := spec.Size
	if size == "" {
		size = snowflakeBaseSize
	}
	if !slices.Contains(snowflakeSizes, size) {
		return nil, fmt.Errorf("snowflake sink: sink.size %q must be one of %v", size, snowflakeSizes)
	}
	component := snowflakeOutput(deps.Output)
	s := &snowflakeSink{
		exec:   deps.Snowflake,
		target: t,
		region: deps.Region,
		size:   size,
		put:    component == "snowflake_put",
		schema: snowflakeSchema(deps.SessionID),
		now:    time.Now,
		sleep:  sleepCtx,
	}
	return s, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// snowflakeOutput finds the first snowflake output anywhere in an output
// config (including inside a broker, switch or fallback) and returns its
// component name. No snowflake output means snowflake_streaming.
func snowflakeOutput(output any) string {
	if name, ok := findSnowflakeOutput(output); ok {
		return name
	}
	return "snowflake_streaming"
}

func findSnowflakeOutput(v any) (string, bool) {
	switch v := v.(type) {
	case map[string]any:
		for _, name := range []string{"snowflake_streaming", "snowflake_put"} {
			if _, ok := v[name]; ok {
				return name, true
			}
		}
		for _, c := range v {
			if name, ok := findSnowflakeOutput(c); ok {
				return name, true
			}
		}
	case []any:
		for _, c := range v {
			if name, ok := findSnowflakeOutput(c); ok {
				return name, true
			}
		}
	}
	return "", false
}

func (s *snowflakeSink) qualified(schema string, obj ...string) string {
	return strings.Join(append([]string{s.target.Database, schema}, obj...), ".")
}

func (s *snowflakeSink) Provision(ctx context.Context, session string) (map[string]string, error) {
	schema := snowflakeSchema(session)
	if err := validSnowflakeIdent(schema); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNothingProvisioned, err)
	}
	s.schema = schema
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS " + s.qualified(schema),
		fmt.Sprintf("ALTER WAREHOUSE %s SET WAREHOUSE_SIZE = %s", s.target.Warehouse, s.size),
	}
	if s.put {
		stmts = append(stmts, "CREATE STAGE IF NOT EXISTS "+s.qualified(schema, snowflakeStage))
	}
	for _, q := range stmts {
		if err := s.exec.Exec(ctx, q); err != nil {
			return nil, fmt.Errorf("snowflake %q: %w", q, err)
		}
	}
	return map[string]string{
		"BENCH_SNOWFLAKE_ACCOUNT":          s.target.Account,
		"BENCH_SNOWFLAKE_USER":             s.target.User,
		"BENCH_SNOWFLAKE_ROLE":             s.target.Role,
		"BENCH_SNOWFLAKE_DATABASE":         s.target.Database,
		"BENCH_SNOWFLAKE_SCHEMA":           schema,
		"BENCH_SNOWFLAKE_WAREHOUSE":        s.target.Warehouse,
		"BENCH_SNOWFLAKE_TABLE":            snowflakeTable,
		"BENCH_SNOWFLAKE_STAGE":            snowflakeStage,
		"BENCH_SNOWFLAKE_PRIVATE_KEY_FILE": snowflakeKeyFile,
	}, nil
}

// Count is the table's row count once it stops moving: Snowpipe Streaming
// commits become visible with a delay, so it re-reads every 10 s until two
// reads agree or 90 s have passed, and returns the last read. The output
// creates the table by schema evolution on its first write, so a missing
// table counts as empty until rows have been seen, and is an error after.
func (s *snowflakeSink) Count(ctx context.Context, _ string) (int64, error) {
	if s.put {
		return 0, ErrCountUnsupported
	}
	q := "SELECT COUNT(*) FROM " + s.qualified(s.schema, snowflakeTable)
	read := func() (int64, error) {
		n, err := s.exec.QueryInt(ctx, q)
		if errors.Is(err, ErrSnowflakeNoObject) && !s.seenRows {
			return 0, nil
		}
		if errors.Is(err, ErrSnowflakeNoObject) {
			return 0, fmt.Errorf("snowflake %q: table missing after rows were written: %w", q, err)
		}
		if err != nil {
			return 0, fmt.Errorf("snowflake %q: %w", q, err)
		}
		if n > 0 {
			s.seenRows = true
		}
		return n, nil
	}
	start := s.now()
	last, err := read()
	if err != nil {
		return 0, err
	}
	for s.now().Sub(start) < snowflakePollMax {
		if err := s.sleep(ctx, snowflakePollInterval); err != nil {
			return 0, err
		}
		n, err := read()
		if err != nil {
			return 0, err
		}
		if n == last {
			return n, nil
		}
		last = n
	}
	return last, nil
}

// BeforePoint re-asserts the run's warehouse size, so a reset by the
// reaper (or anything else) since Provision cannot make a point measure at
// the wrong size.
func (s *snowflakeSink) BeforePoint(ctx context.Context, _ string) error {
	q := fmt.Sprintf("ALTER WAREHOUSE %s SET WAREHOUSE_SIZE = %s", s.target.Warehouse, s.size)
	if err := s.exec.Exec(ctx, q); err != nil {
		return fmt.Errorf("snowflake %q: %w", q, err)
	}
	return nil
}

// Teardown drops the run's schema and everything in it, then puts the
// warehouse back to its base size. Both run, whatever the first returns.
func (s *snowflakeSink) Teardown(ctx context.Context, session string) error {
	schema := snowflakeSchema(session)
	if err := validSnowflakeIdent(schema); err != nil {
		return err
	}
	var errs []error
	for _, q := range []string{
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", s.qualified(schema)),
		fmt.Sprintf("ALTER WAREHOUSE %s SET WAREHOUSE_SIZE = %s", s.target.Warehouse, snowflakeBaseSize),
	} {
		if err := s.exec.Exec(ctx, q); err != nil {
			errs = append(errs, fmt.Errorf("snowflake %q: %w", q, err))
		}
	}
	return errors.Join(errs...)
}

// HostSetup is the bench-script prologue that puts the key on the runner
// host: the host fetches the secret itself, so the key never passes through
// the orchestrator or an SSM parameter, and writes only private_key to a
// 0600 file that the EXIT trap removes when the point's script ends. Nothing
// it runs prints the secret.
func (s *snowflakeSink) HostSetup() string {
	return fmt.Sprintf(`mkdir -p %[1]s
chmod 0700 %[1]s
trap 'rm -f %[2]s' EXIT
rm -f %[2]s
(umask 077 && aws secretsmanager get-secret-value --region %[3]q --secret-id %[4]q --query SecretString --output text | jq -er .private_key > %[2]s)
chmod 0600 %[2]s`, snowflakeKeyDir, snowflakeKeyFile, s.region, snowflakeSecretID)
}

// hostSetupFor is a sink's bench-script prologue, or "" for a sink that
// needs none.
func hostSetupFor(sink Sink) string {
	if h, ok := sink.(interface{ HostSetup() string }); ok {
		return h.HostSetup()
	}
	return ""
}

// snowflakeSecret is the credentials secret's JSON. It is only ever parsed,
// never printed or wrapped into an error.
type snowflakeSecret struct {
	Account    string `json:"account"`
	User       string `json:"user"`
	PrivateKey string `json:"private_key"`
	Database   string `json:"database"`
	Warehouse  string `json:"warehouse"`
	Role       string `json:"role"`
}

// snowflakeOpener connects to the bench account with key-pair auth.
type snowflakeOpener func(t SnowflakeTarget, key *rsa.PrivateKey) (SnowflakeExec, error)

// loadSnowflake reads the credentials secret and opens the orchestrator's
// connection. Errors name the secret and the problem, never its content.
func loadSnowflake(ctx context.Context, sm SecretsManagerClient, open snowflakeOpener) (SnowflakeExec, SnowflakeTarget, error) {
	id := snowflakeSecretID
	out, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &id})
	if err != nil {
		return nil, SnowflakeTarget{}, fmt.Errorf("get snowflake secret %q: %w", id, err)
	}
	if out.SecretString == nil {
		return nil, SnowflakeTarget{}, fmt.Errorf("snowflake secret %q has no SecretString", id)
	}
	var sec snowflakeSecret
	if json.Unmarshal([]byte(*out.SecretString), &sec) != nil {
		return nil, SnowflakeTarget{}, fmt.Errorf("snowflake secret %q is not a JSON object of strings", id)
	}
	t := SnowflakeTarget{Account: sec.Account, User: sec.User, Role: sec.Role, Database: sec.Database, Warehouse: sec.Warehouse}
	for k, v := range map[string]string{"account": t.Account, "user": t.User, "role": t.Role, "database": t.Database, "warehouse": t.Warehouse, "private_key": sec.PrivateKey} {
		if v == "" {
			return nil, SnowflakeTarget{}, fmt.Errorf("snowflake secret %q: %s is missing or empty", id, k)
		}
	}
	// These land in the rendered config, so they must be plain names.
	for k, v := range map[string]string{"account": t.Account, "user": t.User, "role": t.Role} {
		if !snowflakeNameRe.MatchString(v) {
			return nil, SnowflakeTarget{}, fmt.Errorf("snowflake secret %q: %s must match %s", id, k, snowflakeNameRe)
		}
	}
	key, err := parseRSAKey(sec.PrivateKey)
	if err != nil {
		return nil, SnowflakeTarget{}, fmt.Errorf("snowflake secret %q: %w", id, err)
	}
	exec, err := open(t, key)
	if err != nil {
		return nil, SnowflakeTarget{}, fmt.Errorf("open snowflake: %w", err)
	}
	return exec, t, nil
}

// parseRSAKey parses an unencrypted PEM RSA key (PKCS#8 or PKCS#1). Its
// errors carry no key bytes.
func parseRSAKey(p string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(p))
	if block == nil {
		return nil, errors.New("private_key is not PEM")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rk, ok := k.(*rsa.PrivateKey); ok {
			return rk, nil
		}
		return nil, errors.New("private_key is not an RSA key")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	return nil, errors.New("private_key is not an unencrypted PKCS#8 or PKCS#1 RSA key")
}

// openGoSnowflake is the real opener: a connector built from a Config (not
// a DSN string, which would carry the key), JWT auth, no default schema.
func openGoSnowflake(t SnowflakeTarget, key *rsa.PrivateKey) (SnowflakeExec, error) {
	cfg := gosnowflake.Config{
		Account:       t.Account,
		User:          t.User,
		Role:          t.Role,
		Database:      t.Database,
		Warehouse:     t.Warehouse,
		Authenticator: gosnowflake.AuthTypeJwt,
		PrivateKey:    key,
	}
	return &goSnowflake{db: sql.OpenDB(gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, cfg))}, nil
}

type goSnowflake struct{ db *sql.DB }

func (g *goSnowflake) Exec(ctx context.Context, q string) error {
	_, err := g.db.ExecContext(ctx, q)
	return err
}

func (g *goSnowflake) QueryInt(ctx context.Context, q string) (int64, error) {
	var n int64
	err := g.db.QueryRowContext(ctx, q).Scan(&n)
	var se *gosnowflake.SnowflakeError
	if errors.As(err, &se) && se.Number == 2003 {
		return 0, ErrSnowflakeNoObject
	}
	return n, err
}

func (g *goSnowflake) Close() error { return g.db.Close() }
