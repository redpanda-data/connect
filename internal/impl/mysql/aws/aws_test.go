// Copyright 2025 Redpanda Data, Inc.
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

package aws

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeIAMEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		endpoint string
		dsnAddr  string
		want     string
		wantErr  bool
	}{
		{
			name:     "endpoint already has port",
			endpoint: "mydb.rds.amazonaws.com:3306",
			dsnAddr:  "mydb.rds.amazonaws.com:3306",
			want:     "mydb.rds.amazonaws.com:3306",
		},
		{
			name:     "endpoint missing port, derive from DSN",
			endpoint: "mydb.rds.amazonaws.com",
			dsnAddr:  "mydb.rds.amazonaws.com:3307",
			want:     "mydb.rds.amazonaws.com:3307",
		},
		{
			name:     "endpoint missing port, DSN missing port, fall back to 3306",
			endpoint: "mydb.rds.amazonaws.com",
			dsnAddr:  "mydb.rds.amazonaws.com",
			want:     "mydb.rds.amazonaws.com:3306",
		},
		{
			name:     "endpoint empty, fall back to DSN",
			endpoint: "",
			dsnAddr:  "mydb.rds.amazonaws.com:3306",
			want:     "mydb.rds.amazonaws.com:3306",
		},
		{
			name:     "endpoint empty, DSN missing port, append default port",
			endpoint: "",
			dsnAddr:  "mydb.rds.amazonaws.com",
			want:     "mydb.rds.amazonaws.com:3306",
		},
		{
			name:     "endpoint empty and DSN empty errors",
			endpoint: "",
			dsnAddr:  "",
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeIAMEndpoint(tc.endpoint, tc.dsnAddr)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
