package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestOutputJetStreamConfigParse(t *testing.T) {
	spec := natsJetStreamOutputConfig()
	env := service.NewEnvironment()

	t.Run("Successful config parsing", func(t *testing.T) {
		outputConfig := `
urls: [ url1, url2 ]
subject: testsubject
headers:
  Content-Type: application/json
  Timestamp: ${!meta("Timestamp")}
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
  user_jwt: test auth inline user JWT
  user_nkey_seed: test auth inline user NKey Seed
`

		conf, err := spec.ParseYAML(outputConfig, env)
		require.NoError(t, err)

		e, err := newJetStreamWriterFromConfig(conf, service.MockResources())
		require.NoError(t, err)

		msg := service.NewMessage((nil))
		msg.MetaSet("Timestamp", "1651485106")
		assert.Equal(t, "url1,url2", e.connDetails.urls)

		subject, err := e.subjectStr.TryString(msg)
		require.NoError(t, err)
		assert.Equal(t, "testsubject", subject)

		contentType, err := e.headers["Content-Type"].TryString(msg)
		require.NoError(t, err)
		assert.Equal(t, "application/json", contentType)

		timestamp, err := e.headers["Timestamp"].TryString(msg)
		require.NoError(t, err)
		assert.Equal(t, "1651485106", timestamp)

		assert.Equal(t, "test auth n key file", e.connDetails.authConf.NKeyFile)
		assert.Equal(t, "test auth user creds file", e.connDetails.authConf.UserCredentialsFile)
		assert.Equal(t, "test auth inline user JWT", e.connDetails.authConf.UserJWT)
		assert.Equal(t, "test auth inline user NKey Seed", e.connDetails.authConf.UserNkeySeed)
	})

	t.Run("Missing user_nkey_seed", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
subject: testsubject
auth:
  user_jwt: test auth inline user JWT
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
	})

	t.Run("Missing user_jwt", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
subject: testsubject
auth:
  user_jwt: test auth inline user JWT
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
	})
}
