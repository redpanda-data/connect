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

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func TestVectorBloblangV2(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.vector()`)
	require.NoError(t, err)

	res, err := exec.Query([]any{1, 2.5, 3})
	require.NoError(t, err)

	got, ok := res.(vector)
	require.True(t, ok)
	assert.Equal(t, []float32{1, 2.5, 3}, got.value)
}

func TestVectorBloblangV2RejectsNonNumeric(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.vector()`)
	require.NoError(t, err)

	_, err = exec.Query([]any{1, "not a number"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index 1")
}

func TestVectorBloblangV2RejectsNonArrayReceiver(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.vector()`)
	require.NoError(t, err)

	_, err = exec.Query("not an array")
	require.Error(t, err)
}
