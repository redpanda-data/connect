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

package javascript

import (
	"context"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkProcessorBasic(b *testing.B) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    let tmp = benthos.v0_msg_as_structured();
    tmp.sum = tmp.a + tmp.b
    benthos.v0_msg_set_structured(tmp);
  })();
`, nil)
	require.NoError(b, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(b, err)

	tCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	tmpMsg := service.NewMessage(nil)
	tmpMsg.SetStructured(map[string]any{
		"a": 5,
		"b": 7,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		resBatches, err := proc.ProcessBatch(tCtx, service.MessageBatch{tmpMsg.Copy()})
		require.NoError(b, err)
		require.Len(b, resBatches, 1)
		require.Len(b, resBatches[0], 1)

		v, err := resBatches[0][0].AsStructured()
		require.NoError(b, err)
		assert.Equal(b, int64(12), v.(map[string]any)["sum"])
	}

	require.NoError(b, proc.Close(tCtx))
}
