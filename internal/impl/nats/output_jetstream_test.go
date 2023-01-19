package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestOutputJetStreamConfigParse(t *testing.T) {
	spec := natsJetStreamOutputConfig()
	env := service.NewEnvironment()

	outputConfig := `
urls: [ url1, url2 ]
subject: testsubject
headers:
  Content-Type: application/json
  Timestamp: ${!meta("Timestamp")}
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
`

	conf, err := spec.ParseYAML(outputConfig, env)
	require.NoError(t, err)

	e, err := newJetStreamWriterFromConfig(conf, nil, nil)
	require.NoError(t, err)

	msg := service.NewMessage((nil))
	msg.MetaSet("Timestamp", "1651485106")
	assert.Equal(t, "url1,url2", e.urls)
	assert.Equal(t, "testsubject", e.subjectStr.String(msg))
	assert.Equal(t, "application/json", e.headers["Content-Type"].String(msg))
	assert.Equal(t, "1651485106", e.headers["Timestamp"].String(msg))
	assert.Equal(t, "test auth n key file", e.authConf.NKeyFile)
	assert.Equal(t, "test auth user creds file", e.authConf.UserCredentialsFile)
}
