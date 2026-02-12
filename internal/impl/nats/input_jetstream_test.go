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
	"time"

	"github.com/nats-io/nats.go"
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
  token: test auth inline user token
  user: test auth inline user name
  password: test auth inline user password
  user_nkey_seed: test auth inline user NKey Seed
tls_handshake_first: true
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
		assert.Equal(t, "test auth inline user token", e.connDetails.authConf.Token)
		assert.Equal(t, "test auth inline user name", e.connDetails.authConf.User)
		assert.Equal(t, "test auth inline user password", e.connDetails.authConf.Password)
		assert.Equal(t, "test auth inline user NKey Seed", e.connDetails.authConf.UserNkeySeed)
		assert.True(t, e.connDetails.tlsHandshakeFirst)
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

	t.Run("TLS handshake first empty", func(t *testing.T) {
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

		assert.False(t, e.connDetails.tlsHandshakeFirst)
	})
}

func TestAssignMessageMetadata(t *testing.T) {
	t.Run("low values", func(t *testing.T) {
		msg := service.NewMessage([]byte("test"))
		meta := &nats.MsgMetadata{
			Sequence:     nats.SequencePair{Stream: 42, Consumer: 7},
			NumDelivered: 3,
			NumPending:   5,
			Domain:       "testdomain",
			Consumer:     "testconsumer",
			Timestamp:    time.Date(2025, 10, 30, 15, 4, 5, 123456789, time.UTC),
		}

		assignMessageMetadata(meta, msg)

		val, _ := msg.MetaGetMut("nats_sequence_stream")
		assert.Equal(t, "42", val)
		val, _ = msg.MetaGetMut("nats_sequence_consumer")
		assert.Equal(t, "7", val)
		val, _ = msg.MetaGetMut("nats_num_delivered")
		assert.Equal(t, "3", val)
		val, _ = msg.MetaGetMut("nats_num_pending")
		assert.Equal(t, "5", val)
		val, _ = msg.MetaGetMut("nats_domain")
		assert.Equal(t, "testdomain", val)
		val, _ = msg.MetaGetMut("nats_consumer")
		assert.Equal(t, "testconsumer", val)
		val, _ = msg.MetaGetMut("nats_timestamp_unix_nano")
		assert.Equal(t, "1761836645123456789", val)
	})

	t.Run("uint64 values", func(t *testing.T) {
		msg := service.NewMessage([]byte("high"))

		highInt := uint64(18446744073709551615) // Max uint64 0xFFFFFFFFFFFFFFFF
		highIntStr := "18446744073709551615"

		meta := &nats.MsgMetadata{
			Sequence:     nats.SequencePair{Stream: highInt, Consumer: highInt},
			NumDelivered: highInt,
			NumPending:   highInt,
		}

		assignMessageMetadata(meta, msg)

		val, _ := msg.MetaGetMut("nats_sequence_stream")
		assert.Equal(t, highIntStr, val)
		val, _ = msg.MetaGetMut("nats_sequence_consumer")
		assert.Equal(t, highIntStr, val)
		val, _ = msg.MetaGetMut("nats_num_delivered")
		assert.Equal(t, highIntStr, val)
		val, _ = msg.MetaGetMut("nats_num_pending")
		assert.Equal(t, highIntStr, val)
	})
}
