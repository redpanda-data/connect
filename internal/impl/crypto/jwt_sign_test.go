package crypto

import (
	"fmt"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

type testClaims struct {
	Sub  string  `json:"sub"`
	Mood string  `json:"mood"`
	Iat  float64 `json:"iat"`
}

func (c *testClaims) Valid() error {
	return nil
}

func (c *testClaims) MarshalMap() map[string]any {
	return map[string]any{
		"sub":  c.Sub,
		"mood": c.Mood,
		"iat":  c.Iat,
	}
}

func TestBloblangSignJwt(t *testing.T) {
	dummySecretHMAC := "get-in-losers"

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

	inClaims := testClaims{
		Sub:  "1234567890",
		Mood: "Disdainful",
		Iat:  1516239022,
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
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.method, func(t *testing.T) {
			mapping := fmt.Sprintf("root = this.%s(%q)", tc.method, tc.secret)

			exe, err := bloblang.Parse(mapping)
			require.NoError(t, err)

			res, err := exe.Query(inClaims.MarshalMap())
			require.NoError(t, err)

			output, ok := res.(string)
			require.True(t, ok, "bloblang result is not a string")

			var outClaims testClaims
			_, err = jwt.ParseWithClaims(output, &outClaims, func(tok *jwt.Token) (any, error) {
				var key any
				switch tok.Method.(type) {
				case *jwt.SigningMethodHMAC:
					key = []byte(tc.secret)
				case *jwt.SigningMethodRSA:
					privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(tc.secret))
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
