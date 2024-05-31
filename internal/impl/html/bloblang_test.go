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
