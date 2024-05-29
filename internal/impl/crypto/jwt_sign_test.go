package crypto

import (
	"fmt"
	"testing"

	"github.com/golang-jwt/jwt/v5"
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
		tc := tc
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
