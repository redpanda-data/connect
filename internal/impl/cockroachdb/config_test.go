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

package crdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestCRDBConfigParse(t *testing.T) {
	conf := `
cockroach_changefeed:
dsn: postgresql://dan:xxxx@free-tier.gcp-us-central1.cockroachlabs.cloud:26257/defaultdb?sslmode=require&options=--cluster%3Dportly-impala-2852
tables:
    - strm_2
options:
    - UPDATED
    - CURSOR='1637953249519902405.0000000000'
`

	spec := crdbChangefeedInputConfig()
	env := service.NewEnvironment()

	selectConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	selectInput, err := newCRDBChangefeedInputFromConfig(selectConfig, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "EXPERIMENTAL CHANGEFEED FOR strm_2 WITH UPDATED, CURSOR='1637953249519902405.0000000000'", selectInput.statement)
	require.NoError(t, selectInput.Close(t.Context()))
}
