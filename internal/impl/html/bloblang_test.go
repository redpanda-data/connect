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

package html

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestStripHTMLNoArgs(t *testing.T) {
	e, err := bloblang.Parse(`root = this.strip_html()`)
	require.NoError(t, err)

	res, err := e.Query(`<div>meow</div>`)
	require.NoError(t, err)

	assert.Equal(t, "meow", res)
}

func TestStripHTMLWithArgs(t *testing.T) {
	e, err := bloblang.Parse(`root = this.strip_html(["strong","h1"])`)
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
