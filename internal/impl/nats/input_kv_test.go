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

func TestInputKVParse(t *testing.T) {
	spec := natsKVInputConfig()
	env := service.NewEnvironment()

	t.Run("Successful config parsing", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
bucket: testbucket
key: testkey
ignore_deletes: true
include_history: true
meta_only: true
max_reconnects: -1
auth:
  user: test auth inline user name
  password: test auth inline user password
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		e, err := newKVReader(conf, service.MockResources())
		require.NoError(t, err)

		assert.Equal(t, "url1,url2", e.connDetails.urls)
		assert.Equal(t, "testbucket", e.bucket)
		assert.Equal(t, "testkey", e.key)
		assert.True(t, e.ignoreDeletes)
		assert.True(t, e.includeHistory)
		assert.True(t, e.metaOnly)
		assert.Equal(t, -1, *e.connDetails.maxReconnects)
		assert.Equal(t, "test auth inline user name", e.connDetails.authConf.User)
		assert.Equal(t, "test auth inline user password", e.connDetails.authConf.Password)
	})

	t.Run("Missing password", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
bucket: testbucket
auth:
  user: test auth inline user name
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newKVReader(conf, service.MockResources())
		require.ErrorContains(t, err, "missing auth.password")
	})
	t.Run("Missing user", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
bucket: testbucket
auth:
  password: test auth inline user password
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newKVReader(conf, service.MockResources())
		require.ErrorContains(t, err, "missing auth.user")
	})

	t.Run("Multiple auth methods", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
bucket: testbucket
auth:
  token: mytoken
  user: myuser
  password: mypassword
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newKVReader(conf, service.MockResources())
		require.ErrorContains(t, err, "multiple auth methods configured")
	})

	t.Run("Missing user_nkey_seed", func(t *testing.T) {
		inputConfig := `
urls: [ url1, url2 ]
bucket: testbucket
key: testkey
ignore_deletes: true
include_history: true
meta_only: true
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
bucket: testbucket
key: testkey
ignore_deletes: true
include_history: true
meta_only: true
auth:
  user_jwt: test auth inline user JWT
`

		conf, err := spec.ParseYAML(inputConfig, env)
		require.NoError(t, err)

		_, err = newJetStreamReaderFromConfig(conf, service.MockResources())
		require.Error(t, err)
	})
}
