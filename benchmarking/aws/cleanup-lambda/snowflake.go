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
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"github.com/snowflakedb/gosnowflake/v2"
)

// The runner's snowflake sink writes to a per-run schema BENCH_<SESSION> in
// the bench Snowflake account and drops it at teardown. A run that dies
// before teardown leaves the schema behind; the reaper drops any BENCH_
// schema older than the TTL. Snowflake objects are not in the AWS tagging
// index, so they are found by listing and selected by name and created_on.

// snowflakeSchemaRe is the shape of a runner schema: BENCH_ then the
// upper-cased session. It also keeps anything that is not a plain
// identifier out of the DROP statement.
var (
	snowflakeSchemaRe = regexp.MustCompile(`^BENCH_[A-Z0-9_]+$`)
	snowflakeIdentRe  = regexp.MustCompile(`^[A-Z0-9_]+$`)
)

// errSnowflakeSecretMissing means the credentials secret does not exist
// yet: the operator has not set the bench account up, so there is nothing
// to sweep.
var errSnowflakeSecretMissing = errors.New("snowflake secret does not exist")

type snowflakeSchemaInfo struct {
	Name      string
	CreatedOn time.Time
}

// snowflakeReaperAPI is the reaper's slice of Snowflake. Tests fake it.
type snowflakeReaperAPI interface {
	// ListBenchSchemas runs SHOW SCHEMAS LIKE 'BENCH_%' IN DATABASE db.
	ListBenchSchemas(ctx context.Context, db string) ([]snowflakeSchemaInfo, error)
	Exec(ctx context.Context, query string) error
}

// snowflakeOpener connects to the bench account and returns its database
// and warehouse. errSnowflakeSecretMissing when the secret does not exist
// or cannot be read.
type snowflakeOpener func(ctx context.Context) (snowflakeReaperAPI, snowflakeCreds, error)

// sweepSnowflakeSchemas drops stale BENCH_ schemas into report, then puts
// the warehouse back to XSMALL when no run is live (no BENCH_ schema younger
// than ttl), so a crashed run's resize does not keep billing. A nil opener
// (no secret configured) or an unreadable secret skips the sweep; any other
// failure is recorded, not fatal.
func sweepSnowflakeSchemas(ctx context.Context, open snowflakeOpener, now time.Time, ttl time.Duration, report *SweepReport) {
	if open == nil {
		slog.Info("snowflake sweep skipped: no snowflake secret configured")
		return
	}
	api, creds, err := open(ctx)
	if errors.Is(err, errSnowflakeSecretMissing) {
		slog.Warn("snowflake sweep skipped: the snowflake secret is missing or unreadable", "err", err)
		return
	}
	if err != nil {
		report.record("snowflake", "connect", false, err)
		return
	}
	if c, ok := api.(io.Closer); ok {
		defer c.Close()
	}
	db, wh := creds.Database, creds.Warehouse
	for _, id := range []string{db, wh} {
		if !snowflakeIdentRe.MatchString(id) {
			report.record("snowflake", "config", false, fmt.Errorf("identifier %q is not a plain identifier", id))
			return
		}
	}
	schemas, err := api.ListBenchSchemas(ctx, db)
	if err != nil {
		report.record("snowflake-schemas", db, false, err)
		return
	}
	live := false
	for _, s := range schemas {
		if !snowflakeSchemaRe.MatchString(s.Name) {
			continue
		}
		if !olderThanTTL(s.CreatedOn, now, ttl) {
			live = true
			continue
		}
		id := db + "." + s.Name
		err := api.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", id))
		report.record("snowflake-schema", id, err == nil, err)
	}
	if live {
		return
	}
	if err := api.Exec(ctx, fmt.Sprintf("ALTER WAREHOUSE %s SET WAREHOUSE_SIZE = XSMALL", wh)); err != nil {
		report.record("snowflake-warehouse", wh, false, err)
	}
}

// secretsGetter is the slice of Secrets Manager the opener uses.
type secretsGetter interface {
	GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// snowflakeCreds is the non-secret part of the credentials secret.
type snowflakeCreds struct {
	Account, User, Role, Database, Warehouse string
}

// snowflakeConnect opens a connection with key-pair auth.
type snowflakeConnect func(c snowflakeCreds, key *rsa.PrivateKey) (snowflakeReaperAPI, error)

// newSnowflakeOpener reads the credentials secret (JSON {account, user,
// private_key, database, warehouse, role}) on each sweep. Its errors go to
// SNS and Slack, so they name the problem, never the secret's content.
func newSnowflakeOpener(sm secretsGetter, secretID string, connect snowflakeConnect) snowflakeOpener {
	return func(ctx context.Context) (snowflakeReaperAPI, snowflakeCreds, error) {
		none := snowflakeCreds{}
		out, err := sm.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: &secretID})
		if _, ok := errors.AsType[*smtypes.ResourceNotFoundException](err); ok {
			return nil, none, fmt.Errorf("%w: %s not found", errSnowflakeSecretMissing, secretID)
		}
		// Before the secret exists, a grant on the suffixed ARN can answer
		// AccessDenied instead of ResourceNotFound.
		if ae, ok := errors.AsType[smithy.APIError](err); ok && ae.ErrorCode() == "AccessDeniedException" {
			return nil, none, fmt.Errorf("%w: access denied reading %s", errSnowflakeSecretMissing, secretID)
		}
		if err != nil {
			return nil, none, fmt.Errorf("get snowflake secret: %w", err)
		}
		if out.SecretString == nil {
			return nil, none, errors.New("snowflake secret has no SecretString")
		}
		var sec struct {
			Account    string `json:"account"`
			User       string `json:"user"`
			PrivateKey string `json:"private_key"`
			Database   string `json:"database"`
			Warehouse  string `json:"warehouse"`
			Role       string `json:"role"`
		}
		if json.Unmarshal([]byte(*out.SecretString), &sec) != nil {
			return nil, none, errors.New("snowflake secret is not a JSON object of strings")
		}
		c := snowflakeCreds{
			Account: sec.Account, User: sec.User, Role: sec.Role,
			Database: strings.ToUpper(sec.Database), Warehouse: strings.ToUpper(sec.Warehouse),
		}
		if c.Account == "" || c.User == "" || c.Database == "" || c.Warehouse == "" || sec.PrivateKey == "" {
			return nil, none, errors.New("snowflake secret needs account, user, database, warehouse and private_key")
		}
		key, err := parseRSAKey(sec.PrivateKey)
		if err != nil {
			return nil, none, err
		}
		api, err := connect(c, key)
		if err != nil {
			return nil, none, fmt.Errorf("open snowflake: %w", err)
		}
		return api, c, nil
	}
}

// parseRSAKey parses an unencrypted PEM RSA key (PKCS#8 or PKCS#1). Its
// errors carry no key bytes.
func parseRSAKey(p string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(p))
	if block == nil {
		return nil, errors.New("snowflake private_key is not PEM")
	}
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		if rk, ok := k.(*rsa.PrivateKey); ok {
			return rk, nil
		}
		return nil, errors.New("snowflake private_key is not an RSA key")
	}
	if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return k, nil
	}
	return nil, errors.New("snowflake private_key is not an unencrypted PKCS#8 or PKCS#1 RSA key")
}

// connectGoSnowflake builds the connection from a Config, not a DSN string
// (which would carry the key).
func connectGoSnowflake(c snowflakeCreds, key *rsa.PrivateKey) (snowflakeReaperAPI, error) {
	cfg := gosnowflake.Config{
		Account:       c.Account,
		User:          c.User,
		Role:          c.Role,
		Database:      c.Database,
		Warehouse:     c.Warehouse,
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

// ListBenchSchemas reads SHOW SCHEMAS by column name: its column set varies
// across Snowflake releases.
func (g *goSnowflake) ListBenchSchemas(ctx context.Context, db string) ([]snowflakeSchemaInfo, error) {
	rows, err := g.db.QueryContext(ctx, "SHOW SCHEMAS LIKE 'BENCH_%' IN DATABASE "+db)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	nameIdx, createdIdx := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "name":
			nameIdx = i
		case "created_on":
			createdIdx = i
		}
	}
	if nameIdx < 0 || createdIdx < 0 {
		return nil, fmt.Errorf("SHOW SCHEMAS has no name/created_on columns (got %v)", cols)
	}
	var out []snowflakeSchemaInfo
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		name, _ := vals[nameIdx].(string)
		created, ok := vals[createdIdx].(time.Time)
		if !ok {
			s, _ := vals[createdIdx].(string)
			if created, err = time.Parse(time.RFC3339Nano, s); err != nil {
				return nil, fmt.Errorf("schema %s: unparseable created_on %v", name, vals[createdIdx])
			}
		}
		out = append(out, snowflakeSchemaInfo{Name: name, CreatedOn: created})
	}
	return out, rows.Err()
}

func (g *goSnowflake) Close() error { return g.db.Close() }
