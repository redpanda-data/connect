package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestInputJetStreamConfigParse(t *testing.T) {
	spec := natsJetStreamInputConfig()
	env := service.NewEnvironment()

	inputConfig := `
urls: [ url1, url2 ]
subject: testsubject
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
`

	conf, err := spec.ParseYAML(inputConfig, env)
	require.NoError(t, err)

	e, err := newJetStreamReaderFromConfig(conf, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, "url1,url2", e.urls)
	assert.Equal(t, "testsubject", e.subject)
	assert.Equal(t, "test auth n key file", e.authConf.NKeyFile)
	assert.Equal(t, "test auth user creds file", e.authConf.UserCredentialsFile)
}
