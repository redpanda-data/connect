package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestAPIEnableCORS(t *testing.T) {
	conf := api.NewConfig()
	conf.CORS.Enabled = true
	conf.CORS.AllowedOrigins = []string{"*"}

	s, err := api.New("", "", conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	handler := s.Handler()

	request, _ := http.NewRequest("OPTIONS", "/version", http.NoBody)
	request.Header.Add("Origin", "meow")
	request.Header.Add("Access-Control-Request-Method", "POST")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "*", response.Header().Get("Access-Control-Allow-Origin"))
}

func TestAPIEnableCORSOrigins(t *testing.T) {
	conf := api.NewConfig()
	conf.CORS.Enabled = true
	conf.CORS.AllowedOrigins = []string{"foo", "bar"}

	s, err := api.New("", "", conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	handler := s.Handler()

	request, _ := http.NewRequest("OPTIONS", "/version", http.NoBody)
	request.Header.Add("Origin", "foo")
	request.Header.Add("Access-Control-Request-Method", "POST")

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "foo", response.Header().Get("Access-Control-Allow-Origin"))

	request, _ = http.NewRequest("OPTIONS", "/version", http.NoBody)
	request.Header.Add("Origin", "bar")
	request.Header.Add("Access-Control-Request-Method", "POST")

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "bar", response.Header().Get("Access-Control-Allow-Origin"))

	request, _ = http.NewRequest("OPTIONS", "/version", http.NoBody)
	request.Header.Add("Origin", "baz")
	request.Header.Add("Access-Control-Request-Method", "POST")

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "", response.Header().Get("Access-Control-Allow-Origin"))
}

func TestAPIEnableCORSNoHeaders(t *testing.T) {
	conf := api.NewConfig()
	conf.CORS.Enabled = true

	_, err := api.New("", "", conf, nil, log.Noop(), metrics.Noop())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must specify at least one allowed origin")
}

func TestAPIBasicAuth(t *testing.T) {
	secret256 := "K7gNU3sdo+OL0wNhqoVWhr3g6s1xYv72ol/pe/Unols="
	secretMD5 := "Xr4ilOzQ4PCOq3aQ0qbuaQ=="
	secretBcrypt := "JDJhJDEwJHRDMWs2NHc5VjA0R3JIdEE2QzQ2SC5na3o3WWNUUWlQc1RNbVNCUU8yOG8uSWg4YUUvaUcu"
	secretScrypt := "eJ01V0+YCJxHraokLp79ijKS0z1jzVQ3vFtGx5UEEiY="
	type testCase struct {
		name         string
		enabled      bool
		algorithm    string
		correctUser  string
		correctPass  string
		givenUser    string
		givenPass    string
		expectedErr  assert.ErrorAssertionFunc
		expectedCode int
	}

	tests := []testCase{
		{
			name: "validAuth", enabled: true, algorithm: "sha256",
			correctUser: "myuser", correctPass: secret256,
			givenUser: "myuser", givenPass: "secret",
			expectedErr: assert.NoError, expectedCode: http.StatusOK,
		},
		{
			name: "validAuthMD5", enabled: true, algorithm: "md5",
			correctUser: "myuser", correctPass: secretMD5,
			givenUser: "myuser", givenPass: "secret",
			expectedErr: assert.NoError, expectedCode: http.StatusOK,
		},
		{
			name: "validAuthBcrypt", enabled: true, algorithm: "bcrypt",
			correctUser: "myuser", correctPass: secretBcrypt,
			givenUser: "myuser", givenPass: "secret",
			expectedErr: assert.NoError, expectedCode: http.StatusOK,
		},
		{
			name: "validAuthScrypt", enabled: true, algorithm: "scrypt",
			correctUser: "myuser", correctPass: secretScrypt,
			givenUser: "myuser", givenPass: "secret",
			expectedErr: assert.NoError, expectedCode: http.StatusOK,
		},
		{
			name: "invalidAuth", enabled: true, algorithm: "sha256",
			correctUser: "myuser", correctPass: secret256,
			givenUser: "myuser", givenPass: "wrong",
			expectedErr: assert.NoError, expectedCode: http.StatusUnauthorized,
		},
		{
			name: "noAuthGiven", enabled: true, algorithm: "sha256",
			correctUser: "myuser", correctPass: secret256,
			givenUser: "", givenPass: "", expectedErr: assert.NoError,
			expectedCode: http.StatusUnauthorized,
		},
		{
			name: "disabledAuthWrong", enabled: false, algorithm: "sha256",
			correctUser: "myuser", correctPass: secret256,
			givenUser: "myuser", givenPass: "wrong",
			expectedErr: assert.NoError, expectedCode: http.StatusOK,
		},
		{
			name: "disabledAuthNoneGiven", enabled: false, algorithm: "sha256",
			correctUser: "myuser", correctPass: secret256,
			givenUser: "", givenPass: "",
			expectedErr: assert.NoError, expectedCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tc testCase) func(t *testing.T) {
			return func(t *testing.T) {
				conf := api.NewConfig()
				conf.BasicAuth.Enabled = tc.enabled
				conf.BasicAuth.Algorithm = tc.algorithm
				conf.BasicAuth.Username = tc.correctUser
				conf.BasicAuth.PasswordHash = tc.correctPass
				conf.BasicAuth.Salt = "EzrwNJYw2wkErVVV1P36FQ=="

				s, err := api.New("", "", conf, nil, log.Noop(), metrics.Noop())
				if ok := tc.expectedErr(t, err); !(ok && err == nil) {
					return
				}

				handler := s.Handler()

				request, _ := http.NewRequest("GET", "/version", http.NoBody)
				if tc.givenUser != "" || tc.givenPass != "" {
					request.SetBasicAuth(tc.givenUser, tc.givenPass)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)

				assert.Equal(t, tc.expectedCode, response.Code)
			}
		}(tc))
	}
}
