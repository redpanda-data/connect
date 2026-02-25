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

package crypto

import (
	"fmt"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestBloblangSignJwt(t *testing.T) {
	dummySecretHMAC := "dont-tell-anyone"

	// Generated with `openssl genrsa 2048`
	dummySecretRSA := `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAs/ibN8r68pLMR6gRzg4S8v8l6Q7yi8qURjkEbcNeM1rkokC7
xh0I4JVTwxYSVv/JIW8qJdyspl5NIfuAVi32WfKvSAs+NIs+DMsNPYw3yuQals4A
X8hith1YDvYpr8SD44jxhz/DR9lYKZFGhXGB+7NqQ7vpTWp3BceLYocazWJgusZt
7CgecIq57ycM5hjM93BvlrUJ8nQ1a46wfL/8Cy4P0et70hzZrsjjN41KFhKY0iUw
lyU41yEiDHvHDDsTMBxAZosWjSREGfJL6MfpXOInTHs/Gg6DZMkbxjQu6L06EdJ+
Q/NwglJdAXM7Zo9rNELqRig6DdvG5JesdMsO+QIDAQABAoIBAEBo5ixWoe906FVw
6kZjtRZwiIHbjqTHML/dIh+ifzFEA3WqU0m5FHdEGkFEwfWO/83OejgovUWhlFto
JmsxceyJNYBEPdQSTXfIqAlyCHm9n2J/gZTGI8XnxJ8+LHcyjr09QqvT/zDUsX/W
9XVGxW1urcZmFz5UrxpLazAtCEOeqzCRV2Lu05Jk8DWKBWDDjRS24qmWKH1vPSgC
+QuSIHX00OzhE5MuiGgPtE3C/qPzjKLYfvFW7xEN6azZAiIBmIp+Tp9oc8I1CZ/V
buV4iKrkZbGqbZgH4d6FwUuk9NpvYokKn6mFyPYKQJUCwAh4jQhsvsminKeJjci/
xEXIt40CgYEA21PvYT8vWw+gQbUnQsNFa5OBZY8N3YyakgGo3E4EkzjEmE5Ds+R4
kom21PAvFpzY4kxuIJyNYGpvO9RAqh7hflNffTfDL3HRKfG1nAM4V9HOu4P2BFT1
LYmCd8seTQRMZd3rR0zHjWZAos3rrJShESg5oG53lS+DWnptvV1KTWcCgYEA0hAN
i9OpT5hP+p35QLEeeVhHBFlkz/TShssGT1BvKQldEbqTxQtGALfFdvGkYISxzIsj
XpZHd2qfEx/lHiN0xkVz8IOKzS10susMtbcX0ByOBHRxz0+9qloxrP3o2sWVMkf+
vR0/T0kLr1EPgjYb6hNDnQHLOobaNFq8Tu0ZpJ8CgYAMS6ZN01b6SeP4CwnKalwH
7dsBMIXcd7dqnAE1aIJFJpeO2kRdX1+LB4FiapyZLe3SseoyldQvJYha2ElPwC9v
/4iI4olkrYLGUTCXMG8GLVLjnEA8ee7MwLq5sH9gXe9SfqBj/N/rA2J4PgcKQ8LL
zW99mPPHP0Sj290vEn3J3QKBgQCD4iQ/F6KDIIOGO0xUO1+Am9Xqex16GqFak3jg
rwU7ZG+UQ+mmmo9WwAovxUKIfocKfoi0R/GSndRFs46rv2L/YHeMF2o7q0BLXJtc
Mxm2RVc8oMcbe1r+6yWpELjzMX2cVesvXH91Dc1SQrhT7hjUe0fF+WxY0HWKzTTQ
8LdazQKBgGvUgXyLA6Nx0fKr5HvsSHurX67trU7/4GuuOIm+aGx4MWu6E8NZdkxs
tg+1jV0qRszLh20l2jcF5Xr1IUfQINcS2j7v1dGHdBzu9bmupRC7DTYXRiTv+L7L
EppmxRJGlb1Mh0Egvc+eup2lzglmgdRe/FBX4LH6hhH6tohRt8Yx
-----END RSA PRIVATE KEY-----`
	dummySecretECDSA256 := `-----BEGIN EC PRIVATE KEY-----
MHgCAQEEIQD8OkejBIrg9VDaOr3uOQlbqVeCJmz4ewGxtzQ1q7WDhqAKBggqhkjO
PQMBB6FEA0IABBrS6iAXjx5iIUHH9CS4HPhf+Fv6CHadBrWudxt+VXqzQ4FFF5qe
/CdpH4eKi3YdF7ZjjCOfO7Qmqo7wwF37P/8=
-----END EC PRIVATE KEY-----`
	dummySecretECDSA384 := `-----BEGIN EC PRIVATE KEY-----
MIGkAgEBBDBTWmZosMhHGYBLWXLp6OupGWQqUPOeV6N+RNnZuaecYBy6DcK8NiCO
frNZZLLf/eOgBwYFK4EEACKhZANiAARGjPvj8HpLCYuGzxfsJaGetbJGsHXcC5Tw
5h6rLSodG70lY3Dw0hq+qlOa7pc9PjFwVqdiOrwVt64zXV6rToLnaY2ZLgsvDMDa
KaUUSCfzlu8mgLdueS6riZCPC31XF0c=
-----END EC PRIVATE KEY-----`
	dummySecretECDSA512 := `-----BEGIN EC PRIVATE KEY-----
MIHcAgEBBEIA9KQHq4Ta5Spbzgbym9APM+5z+nNeAxVqNy8nOlZo0zVs9hXuSJeQ
0K68oUBLpZkAZ85c8mNiIg6GiDwY5qcQaM6gBwYFK4EEACOhgYkDgYYABACQct22
z0/np8WTKGlhDfUz9K3C3fC+lrGGV+53GdeBM7Ug/hFBGCvJHFnX0RTOG9YNwbcZ
AhySg0xjk96WycIacgFPZeH01CSNYXkrrFLi6kWxsZIDBD4YjLKTkg8nYseA7IxI
JWHmBFldXcJkvNe6PH+6YL1R5jJO3TnNFFa4P6nltg==
-----END EC PRIVATE KEY-----`

	inClaims := jwt.MapClaims{
		"sub":  "1234567890",
		"mood": "Disdainful",
		"iat":  1516239022.0,
	}

	testCases := []struct {
		method string
		secret string
		alg    jwt.SigningMethod
	}{
		{method: "sign_jwt_hs256", secret: dummySecretHMAC, alg: jwt.SigningMethodHS256},
		{method: "sign_jwt_hs384", secret: dummySecretHMAC, alg: jwt.SigningMethodHS384},
		{method: "sign_jwt_hs512", secret: dummySecretHMAC, alg: jwt.SigningMethodHS512},
		{method: "sign_jwt_rs256", secret: dummySecretRSA, alg: jwt.SigningMethodRS256},
		{method: "sign_jwt_rs384", secret: dummySecretRSA, alg: jwt.SigningMethodRS384},
		{method: "sign_jwt_rs512", secret: dummySecretRSA, alg: jwt.SigningMethodRS512},
		{method: "sign_jwt_es256", secret: dummySecretECDSA256, alg: jwt.SigningMethodES256},
		{method: "sign_jwt_es384", secret: dummySecretECDSA384, alg: jwt.SigningMethodES384},
		{method: "sign_jwt_es512", secret: dummySecretECDSA512, alg: jwt.SigningMethodES512},
	}

	for _, tc := range testCases {
		t.Run(tc.method, func(t *testing.T) {
			mapping := fmt.Sprintf("root = this.%s(%q)", tc.method, tc.secret)

			exe, err := bloblang.Parse(mapping)
			require.NoError(t, err)

			res, err := exe.Query(map[string]any(inClaims))
			require.NoError(t, err)

			output, ok := res.(string)
			require.True(t, ok, "bloblang result is not a string")

			var outClaims jwt.MapClaims
			_, err = jwt.ParseWithClaims(output, &outClaims, func(tok *jwt.Token) (any, error) {
				var key any
				switch tok.Method.(type) {
				case *jwt.SigningMethodHMAC:
					key = []byte(tc.secret)
				case *jwt.SigningMethodRSA:
					privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(tc.secret))
					require.NoError(t, err)
					key = privateKey.Public()
				case *jwt.SigningMethodECDSA:
					privateKey, err := jwt.ParseECPrivateKeyFromPEM([]byte(tc.secret))
					require.NoError(t, err)
					key = privateKey.Public()
				default:
					require.Fail(t, "unrecognised signing method")
				}

				if tok.Method.Alg() != tc.alg.Alg() {
					return nil, fmt.Errorf("incorrect signing method: %v", tok.Header["alg"])
				}

				return key, nil
			})
			require.NoError(t, err)
			require.Equal(t, inClaims, outClaims)
		})
	}
}

func TestBloblangSignJwt_WithHeaders(t *testing.T) {
	dummySecretHMAC := "dont-tell-anyone"
	dummySecretRSA := `-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAs/ibN8r68pLMR6gRzg4S8v8l6Q7yi8qURjkEbcNeM1rkokC7
xh0I4JVTwxYSVv/JIW8qJdyspl5NIfuAVi32WfKvSAs+NIs+DMsNPYw3yuQals4A
X8hith1YDvYpr8SD44jxhz/DR9lYKZFGhXGB+7NqQ7vpTWp3BceLYocazWJgusZt
7CgecIq57ycM5hjM93BvlrUJ8nQ1a46wfL/8Cy4P0et70hzZrsjjN41KFhKY0iUw
lyU41yEiDHvHDDsTMBxAZosWjSREGfJL6MfpXOInTHs/Gg6DZMkbxjQu6L06EdJ+
Q/NwglJdAXM7Zo9rNELqRig6DdvG5JesdMsO+QIDAQABAoIBAEBo5ixWoe906FVw
6kZjtRZwiIHbjqTHML/dIh+ifzFEA3WqU0m5FHdEGkFEwfWO/83OejgovUWhlFto
JmsxceyJNYBEPdQSTXfIqAlyCHm9n2J/gZTGI8XnxJ8+LHcyjr09QqvT/zDUsX/W
9XVGxW1urcZmFz5UrxpLazAtCEOeqzCRV2Lu05Jk8DWKBWDDjRS24qmWKH1vPSgC
+QuSIHX00OzhE5MuiGgPtE3C/qPzjKLYfvFW7xEN6azZAiIBmIp+Tp9oc8I1CZ/V
buV4iKrkZbGqbZgH4d6FwUuk9NpvYokKn6mFyPYKQJUCwAh4jQhsvsminKeJjci/
xEXIt40CgYEA21PvYT8vWw+gQbUnQsNFa5OBZY8N3YyakgGo3E4EkzjEmE5Ds+R4
kom21PAvFpzY4kxuIJyNYGpvO9RAqh7hflNffTfDL3HRKfG1nAM4V9HOu4P2BFT1
LYmCd8seTQRMZd3rR0zHjWZAos3rrJShESg5oG53lS+DWnptvV1KTWcCgYEA0hAN
i9OpT5hP+p35QLEeeVhHBFlkz/TShssGT1BvKQldEbqTxQtGALfFdvGkYISxzIsj
XpZHd2qfEx/lHiN0xkVz8IOKzS10susMtbcX0ByOBHRxz0+9qloxrP3o2sWVMkf+
vR0/T0kLr1EPgjYb6hNDnQHLOobaNFq8Tu0ZpJ8CgYAMS6ZN01b6SeP4CwnKalwH
7dsBMIXcd7dqnAE1aIJFJpeO2kRdX1+LB4FiapyZLe3SseoyldQvJYha2ElPwC9v
/4iI4olkrYLGUTCXMG8GLVLjnEA8ee7MwLq5sH9gXe9SfqBj/N/rA2J4PgcKQ8LL
zW99mPPHP0Sj290vEn3J3QKBgQCD4iQ/F6KDIIOGO0xUO1+Am9Xqex16GqFak3jg
rwU7ZG+UQ+mmmo9WwAovxUKIfocKfoi0R/GSndRFs46rv2L/YHeMF2o7q0BLXJtc
Mxm2RVc8oMcbe1r+6yWpELjzMX2cVesvXH91Dc1SQrhT7hjUe0fF+WxY0HWKzTTQ
8LdazQKBgGvUgXyLA6Nx0fKr5HvsSHurX67trU7/4GuuOIm+aGx4MWu6E8NZdkxs
tg+1jV0qRszLh20l2jcF5Xr1IUfQINcS2j7v1dGHdBzu9bmupRC7DTYXRiTv+L7L
EppmxRJGlb1Mh0Egvc+eup2lzglmgdRe/FBX4LH6hhH6tohRt8Yx
-----END RSA PRIVATE KEY-----`
	dummySecretECDSA256 := `-----BEGIN EC PRIVATE KEY-----
MHgCAQEEIQD8OkejBIrg9VDaOr3uOQlbqVeCJmz4ewGxtzQ1q7WDhqAKBggqhkjO
PQMBB6FEA0IABBrS6iAXjx5iIUHH9CS4HPhf+Fv6CHadBrWudxt+VXqzQ4FFF5qe
/CdpH4eKi3YdF7ZjjCOfO7Qmqo7wwF37P/8=
-----END EC PRIVATE KEY-----`
	dummySecretECDSA384 := `-----BEGIN EC PRIVATE KEY-----
MIGkAgEBBDBTWmZosMhHGYBLWXLp6OupGWQqUPOeV6N+RNnZuaecYBy6DcK8NiCO
frNZZLLf/eOgBwYFK4EEACKhZANiAARGjPvj8HpLCYuGzxfsJaGetbJGsHXcC5Tw
5h6rLSodG70lY3Dw0hq+qlOa7pc9PjFwVqdiOrwVt64zXV6rToLnaY2ZLgsvDMDa
KaUUSCfzlu8mgLdueS6riZCPC31XF0c=
-----END EC PRIVATE KEY-----`
	dummySecretECDSA512 := `-----BEGIN EC PRIVATE KEY-----
MIHcAgEBBEIA9KQHq4Ta5Spbzgbym9APM+5z+nNeAxVqNy8nOlZo0zVs9hXuSJeQ
0K68oUBLpZkAZ85c8mNiIg6GiDwY5qcQaM6gBwYFK4EEACOhgYkDgYYABACQct22
z0/np8WTKGlhDfUz9K3C3fC+lrGGV+53GdeBM7Ug/hFBGCvJHFnX0RTOG9YNwbcZ
AhySg0xjk96WycIacgFPZeH01CSNYXkrrFLi6kWxsZIDBD4YjLKTkg8nYseA7IxI
JWHmBFldXcJkvNe6PH+6YL1R5jJO3TnNFFa4P6nltg==
-----END EC PRIVATE KEY-----`

	testCases := []struct {
		name        string
		method      string
		secret      string
		alg         jwt.SigningMethod
		headerArg   string
		errContains string
	}{
		{name: "sign_hs256_invalid_headers", method: "sign_jwt_hs256", secret: dummySecretHMAC, headerArg: "\"not-an-object\"", errContains: "headers parameter must be an object"},
		{name: "sign_rs256_headers_ignored", method: "sign_jwt_rs256", secret: dummySecretRSA, alg: jwt.SigningMethodRS256, headerArg: "{\"alg\": \"none\", \"typ\": \"bar\"}"},
		{name: "sign_rs256_good_and_ignored_headers", method: "sign_jwt_rs256", secret: dummySecretRSA, alg: jwt.SigningMethodRS256, headerArg: "{\"kid\": \"1234\", \"typ\": \"bar\", \"jku\": \"https://www.redpanda.com/keys.json\"}"},
		{name: "sign_rs256_good_and_all_ignored_headers", method: "sign_jwt_rs256", secret: dummySecretRSA, alg: jwt.SigningMethodRS256, headerArg: "{\"kid\": \"1234\", \"alg\": \"none\", \"typ\": \"bar\", \"jku\": \"https://www.redpanda.com/keys.json\", \"jwk\": {\"kty\": \"RSA\"}, \"x5u\": \"https://www.redpanda.com/cert.pem\", \"x5c\": [\"MIICVjCC...base64cert...\"], \"x5t\": \"thumbprint_sha1\", \"x5t#S256\": \"thumbprint_sha256\", \"crit\": [\"badsig\"]}"},
		{name: "sign_hs256_good_headers", method: "sign_jwt_hs256", secret: dummySecretHMAC, alg: jwt.SigningMethodHS256, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_hs384_good_headers", method: "sign_jwt_hs384", secret: dummySecretHMAC, alg: jwt.SigningMethodHS384, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_hs512_good_headers", method: "sign_jwt_hs512", secret: dummySecretHMAC, alg: jwt.SigningMethodHS512, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_rs256_good_headers", method: "sign_jwt_rs256", secret: dummySecretRSA, alg: jwt.SigningMethodRS256, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_rs384_good_headers", method: "sign_jwt_rs384", secret: dummySecretRSA, alg: jwt.SigningMethodRS384, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_rs512_good_headers", method: "sign_jwt_rs512", secret: dummySecretRSA, alg: jwt.SigningMethodRS512, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_es256_good_headers", method: "sign_jwt_es256", secret: dummySecretECDSA256, alg: jwt.SigningMethodES256, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_es384_good_headers", method: "sign_jwt_es384", secret: dummySecretECDSA384, alg: jwt.SigningMethodES384, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
		{name: "sign_es512_good_headers", method: "sign_jwt_es512", secret: dummySecretECDSA512, alg: jwt.SigningMethodES512, headerArg: "{\"kid\": \"1234\", \"foo\": \"bar\"}"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mapping := fmt.Sprintf("root = this.%s(signing_secret: %q, headers: %s)", tc.method, tc.secret, tc.headerArg)

			exe, err := bloblang.Parse(mapping)
			if tc.errContains != "" {
				if err != nil {
					require.Contains(t, err.Error(), tc.errContains)
					return
				}
				_, err = exe.Query(map[string]any{"sub": "user123"})
				require.Error(t, err, "expected an error but got none")
				require.Contains(t, err.Error(), tc.errContains)
				return
			}
			require.NoError(t, err)

			res, err := exe.Query(map[string]any{"sub": "user123"})
			require.NoError(t, err)

			output, ok := res.(string)
			require.True(t, ok, "bloblang result is not a string")

			tok, err := jwt.Parse(output, func(tok *jwt.Token) (any, error) {
				switch tok.Method.(type) {
				case *jwt.SigningMethodHMAC:
					return []byte(tc.secret), nil
				case *jwt.SigningMethodRSA:
					privateKey, perr := jwt.ParseRSAPrivateKeyFromPEM([]byte(tc.secret))
					require.NoError(t, perr)
					return privateKey.Public(), nil
				case *jwt.SigningMethodECDSA:
					privateKey, perr := jwt.ParseECPrivateKeyFromPEM([]byte(tc.secret))
					require.NoError(t, perr)
					return privateKey.Public(), nil
				default:
					return nil, nil
				}
			})
			require.NoError(t, err)
			require.NotNil(t, tok)

			if strings.Contains(tc.headerArg, "kid") {
				assert.Equal(t, "1234", tok.Header["kid"])
			}
			if strings.Contains(tc.headerArg, "foo") {
				assert.Equal(t, "bar", tok.Header["foo"])
			}
			if strings.Contains(tc.headerArg, "alg") {
				assert.NotEqual(t, "none", tok.Header["alg"])
			}
			if strings.Contains(tc.headerArg, "typ") {
				assert.NotEqual(t, "bar", tok.Header["typ"])
			}
			if strings.Contains(tc.headerArg, "jku") {
				assert.NotContains(t, tok.Header, "jku")
			}
			if strings.Contains(tc.headerArg, "jwk") {
				assert.NotContains(t, tok.Header, "jwk")
			}
			if strings.Contains(tc.headerArg, "x5u") {
				assert.NotContains(t, tok.Header, "x5u")
			}
			if strings.Contains(tc.headerArg, "x5c") {
				assert.NotContains(t, tok.Header, "x5c")
			}
			if strings.Contains(tc.headerArg, "x5t") {
				assert.NotContains(t, tok.Header, "x5t")
			}
			if strings.Contains(tc.headerArg, "x5t#S256") {
				assert.NotContains(t, tok.Header, "x5t#S256")
			}
			if strings.Contains(tc.headerArg, "crit") {
				assert.NotContains(t, tok.Header, "crit")
			}

			require.Equal(t, tc.alg.Alg(), tok.Method.Alg())
		})
	}
}
