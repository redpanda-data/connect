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

package javascript

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestProcessorHTTPFetchErrorRedacted(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    benthos.v0_fetch("http://u:S3cretPass@bad host:8080", {}, "GET", "");
  })();
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(t.Context(), time.Second*30)
	defer done()

	_, err = proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("first")),
	})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "S3cretPass")

	require.NoError(t, proc.Close(t.Context()))
}
