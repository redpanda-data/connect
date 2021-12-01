package nats

import (
	"testing"

	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOutputJetStreamConfigParse(t *testing.T) {
	spec := natsJetStreamOutputConfig()
	env := service.NewEnvironment()

	outputConfig := `
urls: [ url1, url2 ]
subject: testsubject
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
`

	conf, err := spec.ParseYAML(outputConfig, env)
	require.NoError(t, err)

	e, err := newJetStreamWriterFromConfig(conf, nil)
	require.NoError(t, err)

	assert.Equal(t, "url1,url2", e.urls)
	assert.Equal(t, "testsubject", e.subjectStr.String(service.NewMessage(nil)))
	assert.Equal(t, "test auth n key file", e.authConf.NKeyFile)
	assert.Equal(t, "test auth user creds file", e.authConf.UserCredentialsFile)
}
