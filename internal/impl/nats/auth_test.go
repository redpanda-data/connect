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
	"testing/fstest"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	NATSUserCreds = `-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJZMzMzT0c1SlFOVzZXU01DNUlMQjY0Uk5UR0hSRExBM1RTNFJGQ1JaMkU3NElYTzVBTU5BIiwiaWF0IjoxNjYxNzkzMjIxLCJpc3MiOiJBQTRJS1VNN0xVTlZLMlNUQ1lWN0lJWlZTWFdBWEhVUEE1RUI1SjNQQ0Y0V1pOSVFUSk1aMlpWTiIsIm5hbWUiOiJ0ZXN0Iiwic3ViIjoiVUE0RkxNRFQySVZNWEQ2SVZVRjRPRFk3UTRTSVBSU0kzVFRLN1ZMR0hFVFNDVUI0SEczQlRYWUUiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQURJQjZKNk40SUNTVlZWWDMzRlc3U1FERlZaSEtLQlhJM05YUkYzWk41WEs1UDI3NVYyWFVKUU4iLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.o11HW6FXVDi8cTA2OcWzYZz3tfiFpDqRNlDEZM0nNg47klTfSBkDW9eTTUC_EsZfaEOpCcy1cafPmBo4vpw_AA
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUABRFVRZW4YPTRCQOFZKF45ISHYBPRXPUV7NHHZJVF3D3M2HLZLDKIJ2U
------END USER NKEY SEED------

*************************************************************`

	NATSUserJWT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJZMzMzT0c1SlFOVzZXU01DNUlMQjY0Uk5UR0hSRExBM1RTNFJGQ1JaMkU3NElYTzVBTU5BIiwiaWF0IjoxNjYxNzkzMjIxLCJpc3MiOiJBQTRJS1VNN0xVTlZLMlNUQ1lWN0lJWlZTWFdBWEhVUEE1RUI1SjNQQ0Y0V1pOSVFUSk1aMlpWTiIsIm5hbWUiOiJ0ZXN0Iiwic3ViIjoiVUE0RkxNRFQySVZNWEQ2SVZVRjRPRFk3UTRTSVBSU0kzVFRLN1ZMR0hFVFNDVUI0SEczQlRYWUUiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1YnMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsImlzc3Vlcl9hY2NvdW50IjoiQURJQjZKNk40SUNTVlZWWDMzRlc3U1FERlZaSEtLQlhJM05YUkYzWk41WEs1UDI3NVYyWFVKUU4iLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.o11HW6FXVDi8cTA2OcWzYZz3tfiFpDqRNlDEZM0nNg47klTfSBkDW9eTTUC_EsZfaEOpCcy1cafPmBo4vpw_AA"
)

func TestNatsAuthConfToOptions(t *testing.T) {
	conf := authConfig{}
	conf.UserCredentialsFile = "user.creds"

	fs := fstest.MapFS{
		"user.creds": {
			Data: []byte(NATSUserCreds),
		},
	}

	options := &nats.Options{}
	optFns := authConfToOptions(conf, service.NewFS(fs))
	for _, fn := range optFns {
		err := fn(options)
		assert.NoError(t, err)
	}

	jwt, err := options.UserJWT()
	assert.NoError(t, err)
	assert.Equal(t, NATSUserJWT, jwt)

	nonce := []byte("that's noncense")
	kp, err := nkeys.ParseDecoratedNKey([]byte(NATSUserCreds))
	assert.NoError(t, err)

	sig, err := kp.Sign(nonce)
	assert.NoError(t, err)

	sigResult, err := options.SignatureCB(nonce)
	assert.NoError(t, err)

	assert.Equal(t, sig, sigResult)
}
