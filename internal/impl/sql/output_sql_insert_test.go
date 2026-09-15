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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestSQLInsertOutputEmptyShutdown(t *testing.T) {
	conf := `
driver: meow
dsn: woof
table: quack
columns: [ foo ]
args_mapping: 'root = [ this.id ]'
`

	spec := sqlInsertOutputConfig()
	env := service.NewEnvironment()

	insertConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	insertOutput, err := newSQLInsertOutputFromConfig(insertConfig, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, insertOutput.Close(t.Context()))
}

// SAP HANA rejects multi-row INSERT ... VALUES (?,?),(?,?), so the hana driver
// must take the per-row prepared statement path like oracle and clickhouse.
func TestSQLInsertHANAUsesPerRowStatements(t *testing.T) {
	conf := `
driver: hana
dsn: hdb://user:pass@host:39017
table: quack
columns: [ foo ]
args_mapping: 'root = [ this.id ]'
`
	env := service.NewEnvironment()

	outConf, err := sqlInsertOutputConfig().ParseYAML(conf, env)
	require.NoError(t, err)
	insertOutput, err := newSQLInsertOutputFromConfig(outConf, service.MockResources())
	require.NoError(t, err)
	require.True(t, insertOutput.useTxStmt, "sql_insert output with driver: hana must use per-row statements")
	require.NoError(t, insertOutput.Close(t.Context()))

	procConf, err := InsertProcessorConfig().ParseYAML(conf, env)
	require.NoError(t, err)
	insertProc, err := NewSQLInsertProcessorFromConfig(procConf, service.MockResources())
	require.NoError(t, err)
	require.True(t, insertProc.useTxStmt, "sql_insert processor with driver: hana must use per-row statements")
	require.NoError(t, insertProc.Close(t.Context()))
}
