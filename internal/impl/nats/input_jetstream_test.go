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

func TestInputJetStreamConfigParse(t *testing.T) {
	spec := natsJetStreamInputConfig()
	env := service.NewEnvironment()

	t.Run("Successful config parsing", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
subject: testsubject
max_reconnects: -1
auth:
  nkey_file: test auth n key file
  user_credentials_file: test auth user creds file
  user_jwt: test auth inline user JWT
  user_nkey_seed: test auth inline user NKey Seed
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		e, err := newJetStreamReaderFromConfig(conf, service.MockResources())
		require.NoError(t, err)

		assert.Equal(t, "url1,url2", e.connDetails.urls)
		assert.Equal(t, "testsubject", e.subject)
		assert.Equal(t, -1, *e.connDetails.maxReconnects)
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

	t.Run("Missing stream and durable for bind", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
subject: testsubject
bind: true
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
	})

	t.Run("Bind set with durable", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
subject: testsubject
durable: foodurable
bind: true
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.NoError(t, err)
	})

	t.Run("Bind set with stream", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
stream: foostream
bind: true
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.NoError(t, err)
	})

	t.Run("Stream set without subject", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
stream: foostream
bind: false
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.NoError(t, err)
	})

	t.Run("Subject set without stream", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
subject: testsubject
bind: false
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.NoError(t, err)
	})

	t.Run("Stream and subject empty", func(t *testing.T) {
		inputConfig := `
urls: [ url1 ]
bind: false
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
	})
}
