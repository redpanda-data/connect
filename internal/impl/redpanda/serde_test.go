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

package redpanda

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStringSerde(t *testing.T) {
	out := make([]byte, 1024)
	n := writeSizedString("foo", out)
	s, amt, err := readSizedString(out[:n])
	require.NoError(t, err)
	require.Equal(t, "foo", s)
	require.Equal(t, n, amt)
}

func TestMessageSerde(t *testing.T) {
	m := transformMessage{
		key:   []byte("abc"),
		value: []byte("123"),
		headers: []transformHeader{
			{key: "foo", value: []byte("bar")},
		},
	}
	out := make([]byte, m.maxSize())
	n := m.serialize(out)
	require.LessOrEqual(t, n, m.maxSize())
	var read transformMessage
	amt, err := read.deserialize(out[:n])
	require.NoError(t, err)
	require.Equal(t, n, amt)
}
