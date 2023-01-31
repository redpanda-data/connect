package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestTestInputKVParse(t *testing.T) {
	spec := natsKVInputConfig()
	env := service.NewEnvironment()

	inputConfig := `
urls: [ url1, url2 ]
bucket: testbucket
key: testkey
ignore_deletes: true
include_history: true
meta_only: true
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
`

	conf, err := spec.ParseYAML(inputConfig, env)
	require.NoError(t, err)

	e, err := newKVReader(conf, service.MockResources())
	require.NoError(t, err)

	assert.Equal(t, "url1,url2", e.urls)
	assert.Equal(t, "testbucket", e.bucket)
	assert.Equal(t, "testkey", e.key)
	assert.Equal(t, true, e.ignoreDeletes)
	assert.Equal(t, true, e.includeHistory)
	assert.Equal(t, true, e.metaOnly)
	assert.Equal(t, "test auth n key file", e.authConf.NKeyFile)
	assert.Equal(t, "test auth user creds file", e.authConf.UserCredentialsFile)
}
