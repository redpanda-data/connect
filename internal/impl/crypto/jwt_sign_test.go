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

func TestBloblangSignJwtHS(t *testing.T) {
	secret := "get-in-losers"
	inClaims := testClaims{
		Sub:  "1234567890",
		Mood: "Disdainful",
		Iat:  1516239022,
	}

	testCases := []struct {
		method string
		alg    *jwt.SigningMethodHMAC
	}{
		{method: "sign_jwt_hs256", alg: jwt.SigningMethodHS256},
		{method: "sign_jwt_hs384", alg: jwt.SigningMethodHS384},
		{method: "sign_jwt_hs512", alg: jwt.SigningMethodHS512},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.method, func(t *testing.T) {
			mapping := fmt.Sprintf("root = this.%s(%q)", tc.method, secret)

			exe, err := bloblang.Parse(mapping)
			require.NoError(t, err)

			res, err := exe.Query(inClaims.MarshalMap())
			require.NoError(t, err)

			output, ok := res.(string)
			require.True(t, ok, "bloblang result is not a string")

			var outClaims testClaims
			_, err = jwt.ParseWithClaims(output, &outClaims, func(tok *jwt.Token) (interface{}, error) {
				if _, ok := tok.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("not an hmac-sha signing method: %v", tok.Header["alg"])
				}

				if tok.Method != tc.alg {
					return nil, fmt.Errorf("incorrect signing method: %v", tok.Header["alg"])
				}

				return []byte(secret), nil
			})
			require.NoError(t, err)
			require.Equal(t, inClaims, outClaims)
		})
	}

}
