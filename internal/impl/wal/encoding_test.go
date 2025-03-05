// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import (
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

func TestEncodingRoundtrip(t *testing.T) {
	expected := service.MessageBatch{
		service.NewMessage([]byte(`hello world!`)),
		service.NewMessage([]byte(`hello test!`)),
		service.NewMessage([]byte(`whoa cool`)),
	}
	expected[0].MetaSetMut("foo", []any{"bar", 3.14, true})
	b, err := appendBatchV0(nil, expected)
	require.NoError(t, err)
	actual, err := readBatch(b)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
