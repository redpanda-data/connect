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

package openai

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerAddressParseErrorRedacted(t *testing.T) {
	pConf, err := embeddingProcessorConfig().ParseYAML(`
server_address: "http://u:S3cretPass@bad host:8080/v1"
api_key: key
model: text-embedding-3-small
`, nil)
	require.NoError(t, err)

	_, err = newBaseProcessor(pConf)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid server_address")
	assert.NotContains(t, err.Error(), "S3cretPass")
}
