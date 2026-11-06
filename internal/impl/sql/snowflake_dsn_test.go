// Copyright 2026 Redpanda Data, Inc.
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

package sql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnowflakeKeyPairDSNFormat pins the key-pair DSN format that the sql_*
// component docs tell users to construct, so that a driver upgrade that
// changes DSN parsing is caught here rather than by users.
func TestSnowflakeKeyPairDSNFormat(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	encodedKey := base64.URLEncoding.EncodeToString(pkcs8)

	dsn := fmt.Sprintf(
		"my_user@my-account.us-east-1/my_db/my_schema?warehouse=my_wh&role=my_role&authenticator=snowflake_jwt&privateKey=%s",
		encodedKey,
	)

	cfg, err := gosnowflake.ParseDSN(dsn)
	require.NoError(t, err)

	assert.Equal(t, "my_user", cfg.User)
	assert.Equal(t, "my-account", cfg.Account)
	assert.Equal(t, "us-east-1", cfg.Region)
	assert.Equal(t, "my_db", cfg.Database)
	assert.Equal(t, "my_schema", cfg.Schema)
	assert.Equal(t, "my_wh", cfg.Warehouse)
	assert.Equal(t, "my_role", cfg.Role)
	assert.Equal(t, gosnowflake.AuthTypeJwt, cfg.Authenticator)
	require.NotNil(t, cfg.PrivateKey)
	assert.True(t, key.Equal(cfg.PrivateKey), "parsed private key should match the encoded one")
}
