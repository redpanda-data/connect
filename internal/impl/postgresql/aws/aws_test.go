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
		dsnHost  string
		dsnPort  uint16
		want     string
		wantErr  bool
	}{
		{
			name:     "endpoint already has port",
			endpoint: "mydb.rds.amazonaws.com:5432",
			dsnHost:  "mydb.rds.amazonaws.com",
			dsnPort:  5432,
			want:     "mydb.rds.amazonaws.com:5432",
		},
		{
			name:     "endpoint missing port, derive from DSN",
			endpoint: "mydb.rds.amazonaws.com",
			dsnHost:  "mydb.rds.amazonaws.com",
			dsnPort:  5433,
			want:     "mydb.rds.amazonaws.com:5433",
		},
		{
			name:     "endpoint missing port, DSN port zero, fall back to 5432",
			endpoint: "mydb.rds.amazonaws.com",
			dsnHost:  "mydb.rds.amazonaws.com",
			dsnPort:  0,
			want:     "mydb.rds.amazonaws.com:5432",
		},
		{
			name:     "endpoint empty, fall back to DSN host:port",
			endpoint: "",
			dsnHost:  "mydb.rds.amazonaws.com",
			dsnPort:  5432,
			want:     "mydb.rds.amazonaws.com:5432",
		},
		{
			name:     "endpoint empty, DSN port zero, append default port",
			endpoint: "",
			dsnHost:  "mydb.rds.amazonaws.com",
			dsnPort:  0,
			want:     "mydb.rds.amazonaws.com:5432",
		},
		{
			name:     "endpoint empty and DSN host empty errors",
			endpoint: "",
			dsnHost:  "",
			dsnPort:  5432,
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeIAMEndpoint(tc.endpoint, tc.dsnHost, tc.dsnPort)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
