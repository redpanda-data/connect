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

package crypto

import (
	"fmt"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func TestBloblangParseJwtHSV2(t *testing.T) {
	secret := "what-is-love"
	expected := map[string]any{
		"sub":  "user1338",
		"name": "Not Blobathan",
	}

	testCases := []struct {
		method      string
		signedValue string
	}{
		{
			method:      "parse_jwt_hs256",
			signedValue: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.EvUOdbPC4jsI_lN265eoidq7b0HrJSlg-DmmBqV_IyE",
		},
		{
			method:      "parse_jwt_hs384",
			signedValue: "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.veULAN-_iRpCZGs6u0CBBh3f77dUtaWAzAbRMoVSImUE9lQ1AvrdY7RT5J4pFjdr",
		},
		{
			method:      "parse_jwt_hs512",
			signedValue: "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.8T55y0w6bP9IBSEjYV6JYw1nQ1BUh5wONhOkoPd4PX4rGaPDMqs0emNouVZih-nqOvjvK0HHqn0OaiaDkaJhug",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.method, func(t *testing.T) {
			exe, err := bloblangv2.Parse(fmt.Sprintf(`output = input.%s(%q)`, tc.method, secret))
			require.NoError(t, err)

			res, err := exe.Query(tc.signedValue)
			require.NoError(t, err)
			require.Equal(t, expected, res)
		})
	}
}

func TestBloblangParseJwtHSV2RejectsNoneAlgorithm(t *testing.T) {
	noneJWT := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJuYW1lIjoiTm90IEJsb2JhdGhhbiIsInN1YiI6InVzZXIxMzM4In0."

	exe, err := bloblangv2.Parse(`output = input.parse_jwt_hs256("what-is-love")`)
	require.NoError(t, err)

	_, err = exe.Query(noneJWT)
	require.ErrorContains(t, err, "incorrect signing method")
}

func TestBloblangParseJwtHSV2WrongSecret(t *testing.T) {
	signed := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.EvUOdbPC4jsI_lN265eoidq7b0HrJSlg-DmmBqV_IyE"

	exe, err := bloblangv2.Parse(`output = input.parse_jwt_hs256("nope")`)
	require.NoError(t, err)

	_, err = exe.Query(signed)
	require.ErrorContains(t, err, "signature is invalid")
}

func TestBloblangSignJwtHSV2RoundTrip(t *testing.T) {
	// Sign then parse through the same secret and assert the claims survive.
	secret := "what-is-love"

	signExec, err := bloblangv2.Parse(fmt.Sprintf(`output = input.sign_jwt_hs256(%q)`, secret))
	require.NoError(t, err)

	signed, err := signExec.Query(map[string]any{"sub": "user1338"})
	require.NoError(t, err)

	parseExec, err := bloblangv2.Parse(fmt.Sprintf(`output = input.parse_jwt_hs256(%q)`, secret))
	require.NoError(t, err)

	got, err := parseExec.Query(signed)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"sub": "user1338"}, got)
}

func TestBloblangSignJwtHSV2WithCustomHeaders(t *testing.T) {
	secret := "what-is-love"

	exec, err := bloblangv2.Parse(fmt.Sprintf(
		`output = input.sign_jwt_hs256(signing_secret: %q, headers: {"kid": "my-key"})`,
		secret,
	))
	require.NoError(t, err)

	res, err := exec.Query(map[string]any{"sub": "user1338"})
	require.NoError(t, err)

	signed, ok := res.(string)
	require.True(t, ok)
	require.NotEmpty(t, signed)

	// Confirm the kid header survived by parsing with the JWT library directly.
	tok, _, err := jwt.NewParser().ParseUnverified(signed, jwt.MapClaims{})
	require.NoError(t, err)
	require.Equal(t, "my-key", tok.Header["kid"])
}
