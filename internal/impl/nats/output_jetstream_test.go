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
max_reconnects: -1
headers:
  Content-Type: application/json
  Timestamp: ${!meta("Timestamp")}
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
  user_jwt: test auth inline user JWT
  token: test auth inline user token
  user: test auth inline user name
  password: test auth inline user password
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

		assert.Equal(t, -1, *e.connDetails.maxReconnects)

		contentType, err := e.headers["Content-Type"].TryString(msg)
		require.NoError(t, err)
		assert.Equal(t, "application/json", contentType)

		timestamp, err := e.headers["Timestamp"].TryString(msg)
		require.NoError(t, err)
		assert.Equal(t, "1651485106", timestamp)

		assert.Equal(t, "test auth n key file", e.connDetails.authConf.NKeyFile)
		assert.Equal(t, "test auth user creds file", e.connDetails.authConf.UserCredentialsFile)
		assert.Equal(t, "test auth inline user JWT", e.connDetails.authConf.UserJWT)
		assert.Equal(t, "test auth inline user token", e.connDetails.authConf.Token)
		assert.Equal(t, "test auth inline user name", e.connDetails.authConf.User)
		assert.Equal(t, "test auth inline user password", e.connDetails.authConf.Password)
		assert.Equal(t, "test auth inline user NKey Seed", e.connDetails.authConf.UserNkeySeed)
	})

	t.Run("Missing password", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
subject: testsubject
auth:
  user: test auth inline user name
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
	})
	t.Run("Missing user", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
subject: testsubject
auth:
  password: test auth inline user password
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
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
