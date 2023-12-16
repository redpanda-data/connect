package httpclient

import (
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleMessageHeaders(t *testing.T) {
	spec := service.NewConfigSpec().Field(ConfigField("GET", false))
	parsed, err := spec.ParseYAML(`
url: example.com/foo
headers:
  "Content-Type": "foo"
metadata:
  include_prefixes: [ "more_" ]
`, nil)
	require.NoError(t, err)

	oldConf, err := ConfigFromParsed(parsed)
	require.NoError(t, err)

	reqCreator, err := RequestCreatorFromOldConfig(oldConf, service.MockResources())
	require.NoError(t, err)

	part := service.NewMessage([]byte("hello world"))
	part.MetaSetMut("more_bar", "barvalue")
	part.MetaSetMut("ignore_baz", "bazvalue")

	b := service.MessageBatch{part}

	req, err := reqCreator.Create(b)
	require.NoError(t, err)

	assert.Equal(t, []string{"foo"}, req.Header.Values("Content-Type"))
	assert.Equal(t, []string{"barvalue"}, req.Header.Values("more_bar"))
	assert.Equal(t, []string(nil), req.Header.Values("ignore_baz"))
}
