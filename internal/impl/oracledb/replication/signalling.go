// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package replication

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/cdc"
)

var _ cdc.Signaler = (*oracleSignaller)(nil)

type oracleSignaller struct {
	db    *sql.DB
	log   *service.Logger
	table UserTable
}

// NewOracleSignaler creates a signaller for the given fully qualified table name
// in <database>.<schema>.<table> format (e.g. "mydb.public.debezium_signal").
func NewOracleSignaler(db *sql.DB, fullyQualifiedName string, log *service.Logger) (*oracleSignaller, error) {
	parts := strings.SplitN(fullyQualifiedName, ".", 3)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid signal table name %q: expected <database>.<schema>.<table>", fullyQualifiedName)
	}

	return &oracleSignaller{
		db:  db,
		log: log,
		table: UserTable{
			Schema: parts[1],
			Name:   parts[2],
		},
	}, nil
}

func (o *oracleSignaller) OnSignal(ctx context.Context, event any) error {
	return nil
}

func (o *oracleSignaller) ValidateChannel(ctx context.Context) error {
	var count int
	const q = `SELECT COUNT(*) FROM all_tables WHERE owner = :1 AND table_name = :2`
	if err := o.db.QueryRowContext(ctx, q, strings.ToUpper(o.table.Schema), strings.ToUpper(o.table.Name)).Scan(&count); err != nil {
		return fmt.Errorf("checking signal table %q exists: %w", o.table.FullName(), err)
	}

	if count == 0 {
		return fmt.Errorf("signal table %q not found", o.table.FullName())
	}
	return nil
}
