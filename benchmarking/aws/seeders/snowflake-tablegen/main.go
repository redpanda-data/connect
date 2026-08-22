// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

// snowflake-tablegen resets and polls the Snowflake tables a sink bench
// writes (the snowflake_streaming counterpart of iceberg-tablegen).
//
//	snowflake-tablegen reset <conn flags> --table=T
//	snowflake-tablegen poll  <conn flags> --tables=T1,T2
//
// reset CREATE OR REPLACEs the bench table with the json-orders schema — one
// statement both drops the old rows (SHOW TABLES restarts at 0) and
// guarantees the table exists. poll prints one metric frame body (per-table
// bytes plus totals) in exactly the format ParseIcebergSeries consumes; it
// reads SHOW TABLES, which is metadata-only and needs no running warehouse,
// so polling every 10s costs no credits.
//
// Auth is key-pair JWT (the only method snowflake_streaming supports): the
// --key-file must be an unencrypted PKCS#8 PEM RSA key.
package main

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"

	sf "github.com/snowflakedb/gosnowflake"
)

// snowflakeIdent guards table names interpolated into DDL. Bench table names
// come from BenchNames (dash-sanitized session id + connector + engine), so
// anything outside this alphabet is a bug upstream, not a quoting problem to
// solve here.
var snowflakeIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "reset" && os.Args[1] != "poll") {
		fmt.Fprintln(os.Stderr, "usage: snowflake-tablegen reset|poll --account= --user= --role= --database= --schema= --key-file= [--table= | --tables=]")
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	account := fs.String("account", "", "Snowflake account identifier (ORG-ACCOUNT)")
	user := fs.String("user", "", "user to connect as")
	role := fs.String("role", "", "role with CREATE TABLE on the schema")
	database := fs.String("database", "", "database holding the bench schema")
	schema := fs.String("schema", "", "schema the bench tables live in")
	keyFile := fs.String("key-file", "", "PKCS#8 PEM RSA private key file (unencrypted)")
	table := fs.String("table", "", "table to reset (reset)")
	tables := fs.String("tables", "", "comma-separated tables to poll (poll)")
	_ = fs.Parse(os.Args[2:])

	for name, v := range map[string]string{
		"account": *account, "user": *user, "role": *role,
		"database": *database, "schema": *schema, "key-file": *keyFile,
	} {
		if v == "" {
			fmt.Fprintf(os.Stderr, "snowflake-tablegen: --%s is required\n", name)
			os.Exit(2)
		}
	}

	key, err := loadPrivateKey(*keyFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "snowflake-tablegen: load key:", err)
		os.Exit(1)
	}
	cfg := sf.Config{
		Account:       *account,
		User:          *user,
		Role:          *role,
		Database:      *database,
		Schema:        *schema,
		Authenticator: sf.AuthTypeJwt,
		PrivateKey:    key,
	}
	dsn, err := sf.DSN(&cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "snowflake-tablegen: build DSN:", err)
		os.Exit(1)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "snowflake-tablegen: open:", err)
		os.Exit(1)
	}
	defer db.Close()

	switch cmd {
	case "reset":
		if err := reset(db, *table); err != nil {
			fmt.Fprintln(os.Stderr, "snowflake-tablegen: reset:", err)
			os.Exit(1)
		}
	case "poll":
		names := strings.Split(*tables, ",")
		frame, err := poll(db, names)
		if err != nil {
			fmt.Fprintln(os.Stderr, "snowflake-tablegen: poll:", err)
			os.Exit(1)
		}
		fmt.Print(frame)
	}
}

// loadPrivateKey reads an unencrypted PKCS#8 PEM RSA key (the .p8 format
// Snowflake key-pair auth docs produce with `openssl genrsa | pkcs8 -nocrypt`).
func loadPrivateKey(path string) (*rsa.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return nil, fmt.Errorf("%s is not PEM", path)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#8 (encrypted keys are not supported): %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%s holds a %T, want RSA", path, parsed)
	}
	return key, nil
}

// reset CREATE OR REPLACEs the table with the json-orders schema (see
// seeders/json-orders/produce.go and iceberg-tablegen's ordersSchema — the
// three must stay in agreement). Column names are unquoted so Snowflake
// uppercases them, matching how snowflake_streaming normalizes the JSON keys
// (internal/impl/snowflake/streaming/compat.go); schema evolution stays off
// in the bench scenarios, so any drift here fails loud instead of silently
// ALTERing mid-run. DDL needs no running warehouse.
func reset(db *sql.DB, table string) error {
	if !snowflakeIdent.MatchString(table) {
		return fmt.Errorf("table %q is not a plain identifier", table)
	}
	ddl := fmt.Sprintf(`CREATE OR REPLACE TABLE %s (
  ID NUMBER,
  TS VARCHAR,
  REGION VARCHAR,
  AMOUNT DOUBLE,
  STATUS VARCHAR,
  PAYLOAD VARCHAR
)`, table)
	if _, err := db.Exec(ddl); err != nil {
		return err
	}
	fmt.Printf("snowflake-tablegen: reset %s\n", table)
	return nil
}

// tableStat is one table's SHOW TABLES metadata snapshot.
type tableStat struct {
	rows  int64
	bytes int64
}

// poll reads SHOW TABLES (session database/schema scope) and renders the
// frame body for the requested tables. A table SHOW doesn't list (e.g. the
// first poll racing the reset) counts as zero rather than erroring: the
// sidecar treats a non-zero exit as a lost sample, and an all-zero frame is
// the more honest record of "nothing committed yet".
func poll(db *sql.DB, tables []string) (string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	idx := map[string]int{}
	for i, c := range cols {
		idx[strings.ToLower(c)] = i
	}
	for _, need := range []string{"name", "rows", "bytes"} {
		if _, ok := idx[need]; !ok {
			return "", fmt.Errorf("SHOW TABLES has no %q column (got %v)", need, cols)
		}
	}
	stats := map[string]tableStat{}
	vals := make([]any, len(cols))
	for i := range vals {
		vals[i] = new(any)
	}
	for rows.Next() {
		if err := rows.Scan(vals...); err != nil {
			return "", err
		}
		name := fmt.Sprint(*vals[idx["name"]].(*any))
		stats[strings.ToUpper(name)] = tableStat{
			rows:  toInt64(*vals[idx["rows"]].(*any)),
			bytes: toInt64(*vals[idx["bytes"]].(*any)),
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return formatFrame(tables, stats), nil
}

// formatFrame renders one metric frame body in ParseIcebergSeries format:
// a per-table evidence line for each requested table (inert to the parser,
// same rationale as the iceberg sidecar's), then the summed totals the
// parser derives throughput from. Tables render in the order requested;
// lookups are case-insensitive because SHOW TABLES reports uppercase names.
func formatFrame(tables []string, stats map[string]tableStat) string {
	var sb strings.Builder
	var totalBytes, totalRows int64
	for _, t := range tables {
		st := stats[strings.ToUpper(strings.TrimSpace(t))]
		fmt.Fprintf(&sb, "table_files_size_bytes %s %d\n", strings.TrimSpace(t), st.bytes)
		totalBytes += st.bytes
		totalRows += st.rows
	}
	fmt.Fprintf(&sb, "total_files_size_bytes %d\n", totalBytes)
	fmt.Fprintf(&sb, "total_records %d\n", totalRows)
	return sb.String()
}

// toInt64 coerces the driver's SHOW TABLES cell values, which arrive as
// string, int64, or float64 depending on gosnowflake version.
func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case string:
		var n int64
		_, _ = fmt.Sscan(x, &n)
		return n
	default:
		return 0
	}
}
