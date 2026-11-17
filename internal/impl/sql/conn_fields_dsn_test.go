// Copyright 2026 Redpanda Data, Inc.
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReworkOldClickhouseDSN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "username and password",
			in:   "tcp://host:9000?database=db&username=u&password=p&debug=true",
			want: "clickhouse://u:p@host:9000/db?debug=true",
		},
		{
			name: "username only",
			in:   "tcp://host:9000?database=db&username=u",
			want: "clickhouse://u@host:9000/db",
		},
		{
			name: "no credentials",
			in:   "tcp://host:9000?database=db",
			want: "clickhouse://host:9000/db",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := reworkOldClickhouseDSN(test.in)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestReworkOldClickhouseDSNRedactsParseError(t *testing.T) {
	_, err := reworkOldClickhouseDSN("tcp://bad host:9000?username=u&password=S3cretPass")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestSQLOpenWithReworksRedactsDriverErrors(t *testing.T) {
	const pass = "S3cretPass"
	for _, test := range []struct {
		driver string
		dsn    string
	}{
		{"postgres", "postgres://u:" + pass + "@bad host:5432/db"},
		{"clickhouse", "clickhouse://u:" + pass + "@bad host:9000/db"},
		{"clickhouse", "tcp://bad host:9000?username=u&password=" + pass},
		{"mssql", "sqlserver://u:" + pass + "@bad host:1433?database=db"},
		{"oracle", "oracle://u:" + pass + "@bad host:1521/service"},
		{"trino", "http://u:" + pass + "@bad host:8080?catalog=c"},
	} {
		t.Run(test.driver, func(t *testing.T) {
			db, err := sqlOpenWithReworks(nil, test.driver, test.dsn)
			if err == nil {
				t.Cleanup(func() { _ = db.Close() })
				err = db.PingContext(t.Context())
			}
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "unknown driver")
			assert.NotContains(t, err.Error(), pass)
		})
	}
}
