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

package sql

import (
	"context"
	"database/sql"
	"strings"
)

func splitSQLStatements(statement string) []string {
	var result []string
	startp := 0
	p := 0
	sawNonCommentOrSpace := false
	for {
		if p == len(statement) || statement[p] == ';' {
			if p != len(statement) && statement[p] == ';' {
				// include trailing semicolon
				p++
			}
			statementPart := statement[startp:p]
			if sawNonCommentOrSpace {
				result = append(result, strings.TrimSpace(statementPart))
			} else {
				// coalesce any functionally "empty" statements into the previous statement
				// so any configurations that have something like "statement; -- final comment"
				// will still work
				result[len(result)-1] += statementPart
			}
			if p == len(statement) {
				break
			}
			startp = p
			sawNonCommentOrSpace = false
		} else if statement[p] == '\'' || statement[p] == '"' || statement[p] == '`' {
			// single-quoted strings, double-quoted identifiers, and backtick-quoted identifiers
			sentinel := statement[p]
			p++
			for p < len(statement) && statement[p] != sentinel {
				p++
			}
			sawNonCommentOrSpace = true
		} else if statement[p] == '#' ||
			(p+1 < len(statement) && statement[p:p+2] == "--") ||
			(p+1 < len(statement) && statement[p:p+2] == "//") {
			// single-line comments starting with hash, double-dash, or double-slash
			for p < len(statement) && statement[p] != '\n' {
				p++
			}
		} else if p+1 < len(statement) && statement[p:p+2] == "/*" {
			// multi-line comments starting with slash-asterisk
			for p+1 < len(statement) && statement[p:p+2] != "*/" {
				p++
			}
		} else if !(statement[p] == ' ' || statement[p] == '\t' || statement[p] == '\r' || statement[p] == '\n') {
			sawNonCommentOrSpace = true
		}
		if p != len(statement) {
			p++
		}
	}

	return result
}

func execMultiWithContext(db *sql.DB, ctx context.Context, query string, args ...any) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	statements := splitSQLStatements(query)
	for _, part := range statements {
		if _, err = tx.ExecContext(ctx, part, args...); err != nil {
			return err
		}
		args = []any{}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	// TODO: should this return anything for a result?
	return nil
}

func queryMultiWithContext(db *sql.DB, ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	statements := splitSQLStatements(query)
	var rows *sql.Rows
	for i, part := range statements {
		// this may not be useful to only give the args to the first query. but, principle of least surprise,
		// make it act the same way that execMultiWithContext and the various drivers do.
		if i < len(statements)-1 {
			if _, err = tx.ExecContext(ctx, part, args...); err != nil {
				return nil, err
			}
		} else {
			rows, err = tx.QueryContext(ctx, part, args...)
			if err != nil {
				return nil, err
			}
		}
		args = []any{}
	}

	if err = tx.Commit(); err != nil {
		return nil, err
	}

	return rows, nil
}
