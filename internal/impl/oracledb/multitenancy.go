// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package oracledb

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

// validOracleIdentifier matches a valid unquoted Oracle identifier: starts with a
// letter, followed by letters, digits, _, $, or #. Used to validate pdb_name at
// parse time before it reaches SQL construction sites.
var validOracleIdentifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_$#]*$`)

// detectContainerContext queries Oracle to determine which container the connection
// landed in. Returns true if connected to CDB$ROOT (CDB mode). If pdb_name is set
// but the connection is not at CDB$ROOT, an error is returned — pdb_name requires
// connecting via the CDB root service.
func (o *oracleDBCDCInput) detectContainerContext(ctx context.Context) (bool, error) {
	if o.cfg.PDBName == "" {
		return false, nil
	}

	var conName string
	if err := o.db.QueryRowContext(ctx, `SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL`).Scan(&conName); err != nil {
		return false, fmt.Errorf("detecting oracle container context: %w", err)
	}
	o.log.Infof("Connected to Oracle container: %s", conName)

	if strings.EqualFold(conName, "CDB$ROOT") {
		o.log.Infof("CDB-mode: will use ALTER SESSION SET CONTAINER = %s for catalog queries", o.cfg.PDBName)
		return true, nil
	}

	return false, fmt.Errorf("pdb_name is set but connected to container '%s' instead of CDB$ROOT; connect via the CDB root service or remove pdb_name", conName)
}

// cdbCheckpointTable returns the checkpoint cache table name to use in CDB mode.
// When connected at CDB$ROOT the checkpoint table lives under the common user C##RPCN,
// so the auto-derived name (which uses the plain RPCN prefix) needs a C## prefix.
// At parse time we don't yet know whether the connection is CDB or PDB-direct, so this
// fixup is applied after detectContainerContext confirms CDB mode.
func cdbCheckpointTable(tableName string) string {
	if strings.HasPrefix(tableName, "RPCN.CDC_CHECKPOINT_") {
		return "C##" + tableName
	}
	return tableName
}
