package crypto

import (
	"fmt"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestBloblangParseJwtHS(t *testing.T) {
	secret := "what-is-love"
	expected := map[string]any{
		"sub":  "user1338",
		"name": "Not Blobathan",
	}

	testCases := []struct {
		method      string
		alg         *jwt.SigningMethodHMAC
		signedValue string
	}{
		{
			method: "parse_jwt_hs256", alg: jwt.SigningMethodHS256,
			signedValue: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.EvUOdbPC4jsI_lN265eoidq7b0HrJSlg-DmmBqV_IyE",
		},
		{
			method: "parse_jwt_hs384", alg: jwt.SigningMethodHS384,
			signedValue: "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.veULAN-_iRpCZGs6u0CBBh3f77dUtaWAzAbRMoVSImUE9lQ1AvrdY7RT5J4pFjdr",
		},
		{
			method: "parse_jwt_hs512", alg: jwt.SigningMethodHS512,
			signedValue: "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.8T55y0w6bP9IBSEjYV6JYw1nQ1BUh5wONhOkoPd4PX4rGaPDMqs0emNouVZih-nqOvjvK0HHqn0OaiaDkaJhug",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.method, func(t *testing.T) {
			mapping := fmt.Sprintf("root = this.%s(%q)", tc.method, secret)

			exe, err := bloblang.Parse(mapping)
			require.NoError(t, err)

			res, err := exe.Query(tc.signedValue)
			require.NoError(t, err)
			require.Equal(t, expected, res)
		})
	}
}

// This is a test to ensure the parsing logic is safe against the None attack
// regardless of the safeguards provided by JWT library in use. See:
// https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/
func TestBloblangParseJwtHS_RejectNoneAlgorithm(t *testing.T) {
	terribleJWT := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJuYW1lIjoiTm90IEJsb2JhdGhhbiIsInN1YiI6InVzZXIxMzM4In0."

	mapping := fmt.Sprintf("root = this.parse_jwt_hs256(%q)", "what-is-love")

	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(terribleJWT)
	require.ErrorIs(t, err, errJWTUnrecognizedMethod)
	require.Nil(t, res)
}

func TestBloblangParseJwtHS_RejectIncorrectHSAlgorithm(t *testing.T) {
	terribleJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.EvUOdbPC4jsI_lN265eoidq7b0HrJSlg-DmmBqV_IyE"

	mapping := fmt.Sprintf("root = this.parse_jwt_hs384(%q)", "what-is-love")

	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(terribleJWT)
	require.ErrorIs(t, err, errJWTIncorrectMethod)
	require.Nil(t, res)
}

func TestBloblangParseJwtHS_WrongSecret(t *testing.T) {
	terribleJWT := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.EvUOdbPC4jsI_lN265eoidq7b0HrJSlg-DmmBqV_IyE"

	mapping := fmt.Sprintf("root = this.parse_jwt_hs256(%q)", "nope")

	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(terribleJWT)
	require.ErrorIs(t, err, jwt.ErrSignatureInvalid)
	require.Nil(t, res)
}
