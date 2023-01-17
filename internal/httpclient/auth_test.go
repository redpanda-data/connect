package httpclient

import (
	"testing"

	"github.com/golang-jwt/jwt"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthConfigParsing(t *testing.T) {
	spec := service.NewConfigSpec()
	for _, v := range AuthFieldSpecs() {
		spec.Field(v)
	}

	parsedConf, err := spec.ParseYAML(`
basic_auth:
  enabled: true
  username: basicuser
  password: basicpass

oauth:
  enabled: true
  consumer_key: fookey
  consumer_secret: foosecret
  access_token: footoken
  access_token_secret: footokensecret

jwt:
  enabled: true
  claims:
    foo: fooclaim
    bar: 2
  headers:
    baz: bazheader
    buz: 3
  signing_method: foomethod
  private_key_file: fookeyfile
`, service.NewEnvironment())
	require.NoError(t, err)

	authConf, err := authConfFromParsed(parsedConf)
	require.NoError(t, err)

	assert.True(t, authConf.BasicAuth.Enabled)
	assert.Equal(t, "basicuser", authConf.BasicAuth.Username)
	assert.Equal(t, "basicpass", authConf.BasicAuth.Password)

	assert.True(t, authConf.OAuth.Enabled)
	assert.Equal(t, "fookey", authConf.OAuth.ConsumerKey)
	assert.Equal(t, "foosecret", authConf.OAuth.ConsumerSecret)
	assert.Equal(t, "footoken", authConf.OAuth.AccessToken)
	assert.Equal(t, "footokensecret", authConf.OAuth.AccessTokenSecret)

	assert.True(t, authConf.JWT.Enabled)
	assert.Equal(t, jwt.MapClaims{
		"foo": "fooclaim",
		"bar": 2,
	}, authConf.JWT.Claims)
	assert.Equal(t, map[string]any{
		"baz": "bazheader",
		"buz": 3,
	}, authConf.JWT.Headers)
	assert.Equal(t, "foomethod", authConf.JWT.SigningMethod)
	assert.Equal(t, "fookeyfile", authConf.JWT.PrivateKeyFile)
}
