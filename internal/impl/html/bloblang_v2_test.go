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

package html

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func TestStripHTMLBloblangV2NoArgs(t *testing.T) {
	e, err := bloblangv2.Parse(`output = input.strip_html()`)
	require.NoError(t, err)

	res, err := e.Query(`<div>meow</div>`)
	require.NoError(t, err)

	assert.Equal(t, "meow", res)
}

func TestStripHTMLBloblangV2WithArgs(t *testing.T) {
	e, err := bloblangv2.Parse(`output = input.strip_html(["strong","h1"])`)
	require.NoError(t, err)

	res, err := e.Query(`<div>
  <h1>meow</h1>
  <p>hello world this is <strong>some</strong> text.
</div>`)
	require.NoError(t, err)

	assert.Equal(t, `
  <h1>meow</h1>
  hello world this is <strong>some</strong> text.
`, res)
}

func TestStripHTMLBloblangV2AcceptsBytesReceiver(t *testing.T) {
	e, err := bloblangv2.Parse(`output = input.strip_html()`)
	require.NoError(t, err)

	res, err := e.Query([]byte(`<div>meow</div>`))
	require.NoError(t, err)
	assert.Equal(t, "meow", res)
}

func TestStripHTMLBloblangV2RejectsNonStringReceiver(t *testing.T) {
	e, err := bloblangv2.Parse(`output = input.strip_html()`)
	require.NoError(t, err)

	_, err = e.Query(42)
	require.Error(t, err)
}
