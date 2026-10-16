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

package doris

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBadURL = "http://u:S3cretPass@bad host:8030"

func TestConfigFEURLParseErrorRedacted(t *testing.T) {
	pConf, err := dorisStreamLoadSpec().ParseYAML(`
fe_urls: [ "`+testBadURL+`" ]
database: test
table: foo
username: root
password: ""
`, nil)
	require.NoError(t, err)

	_, err = dorisStreamLoadConfigFromParsed(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing fe_urls[0]")
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestStreamLoadURLsParseErrorRedacted(t *testing.T) {
	_, err := streamLoadURLs([]string{testBadURL}, "db", "tbl")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}

func TestConnectionCheckParseErrorRedacted(t *testing.T) {
	d := &dorisStreamLoadOutput{}
	err := d.connectionCheck(t.Context(), testBadURL, 9030)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")
}
