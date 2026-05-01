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

	"github.com/redpanda-data/connect/v4/internal/bloblang/migratortest"
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

func TestBloblangParseJwtRSV2(t *testing.T) {
	expected := map[string]any{
		"sub":  "user1338",
		"name": "Not Blobathan",
	}

	testCases := []struct {
		method      string
		signedValue string
	}{
		{
			method:      "parse_jwt_rs256",
			signedValue: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA",
		},
		{
			method:      "parse_jwt_rs384",
			signedValue: "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.detziSnNZJ0cX75pof0EASsajqCmes4otwSYAMjVdr31-gADaGdXTKrkpClUeFdH_488UaekpaeP1iRzML8-kp1yGa6ZCfOw1E_r3zT6hkdZwPDi5OKQy2V5JWlvGTzzwfSc9SgaRGyGg-FBo54CakQMwAA3Us_g82sy4bwO1ay2BriW5dX6tJnm2875DgBzOlHnAt97bH0odT7_LbJPkm9c_H7EdVUH810Qar_NVaPdVgwo5CMN4lCXxIjrFoxCJ3kEu8jf-9bZedK5UHsRlo7lYDxtxrmi9izMXvwCbEcn4Hgi6a_SjsOzsHYriRJN5NCQI_vs4kFiUWiLAyFNeA",
		},
		{
			method:      "parse_jwt_rs512",
			signedValue: "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.eePFKSyF7LHAOehfEKi-V1cOUj5rtHPZ6uyj9VLlihOOyL8jPrny_8w9tsF4YC0jFzsKeRQ2Nnb8_IZqqWhbJgtfUOtkdl4G4CaLEJPUZH3kD_AvVQMsQGjsLO4Mu_rNycLByqk0RZjRVxNTkkt_ArZVSiLX9tmkvvT5fvHTfoGSe56qdhjrzyIcICckwdZU3AJTMf8w3loDISQLEG4OufkrmERXvslAkPN1ZxCZdwg7SHnATz8iEFerGiU-4QNN5dOuQi_XIdPMIbKE6dp4cYDyyr5wVnaEOCDd_TEEenpRLeHsqka3hmQY45rDiOXznpIkpZWeFNmf-4yjVHCZVg",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.method, func(t *testing.T) {
			exe, err := bloblangv2.Parse(fmt.Sprintf(`output = input.%s(%q)`, tc.method, dummySecretRSA))
			require.NoError(t, err)

			res, err := exe.Query(tc.signedValue)
			require.NoError(t, err)
			require.Equal(t, expected, res)
		})
	}
}

func TestBloblangParseJwtRSV2RejectsNoneAlgorithm(t *testing.T) {
	noneJWT := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0."

	exe, err := bloblangv2.Parse(fmt.Sprintf(`output = input.parse_jwt_rs256(%q)`, dummySecretRSA))
	require.NoError(t, err)

	_, err = exe.Query(noneJWT)
	require.ErrorContains(t, err, "incorrect signing method")
}

func TestBloblangParseJwtRSV2WrongSecret(t *testing.T) {
	signed := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA"

	exe, err := bloblangv2.Parse(fmt.Sprintf(`output = input.parse_jwt_rs256(%q)`, dummyWrongSecretRSA))
	require.NoError(t, err)

	_, err = exe.Query(signed)
	require.ErrorContains(t, err, "verification error")
}

func TestBloblangParseJwtECV2(t *testing.T) {
	expected := map[string]any{
		"sub":  "1234567890",
		"mood": "Disdainful",
		"iat":  1.516239022e+09,
	}

	testCases := []struct {
		method      string
		signedValue string
		dummySecret string
	}{
		{
			method:      "parse_jwt_es256",
			signedValue: "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.-8LrOdkEiv_44ADWW08lpbq41ZmHCel58NMORPq1q4Dyw0zFhqDVLrRoSvCvuyyvgXAFb9IHfR-9MlJ_2ShA9A",
			dummySecret: dummySecretECDSA256,
		},
		{
			method:      "parse_jwt_es384",
			signedValue: "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.bkrqALC-HuAOXYiH4Xdc6gT5-tgRY9niI5bB0luuIBkyYRKHwNLtFIZ-lw54ld3_20BxXNaC-o6zFJwTEUaqZybRBj2KZtV8X7cX1oKte_V4YceNYESnmqiEP0eA7PHh",
			dummySecret: dummySecretECDSA384,
		},
		{
			method:      "parse_jwt_es512",
			signedValue: "eyJhbGciOiJFUzUxMiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.AET5FhyU_Y0gB2QZ7cMxTY_o6ioMEuBz9MliILqE1En3AjiBdWyVwtuSva-u0WVuTIQmpV3Uaes0_DNhSRoBa3jzAKElAJzNlF0D_reofCTfwfTur4XuRHOCRCU9UFHuATMwIUd_me7aF3K4fQKu1OuaGjZT8F3R2usoiZVMjm9e-bw5",
			dummySecret: dummySecretECDSA512,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.method, func(t *testing.T) {
			exe, err := bloblangv2.Parse(fmt.Sprintf(`output = input.%s(%q)`, tc.method, tc.dummySecret))
			require.NoError(t, err)

			res, err := exe.Query(tc.signedValue)
			require.NoError(t, err)
			require.Equal(t, expected, res)
		})
	}
}

func TestBloblangParseJwtECV2RejectsNoneAlgorithm(t *testing.T) {
	noneJWT := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0."

	exe, err := bloblangv2.Parse(fmt.Sprintf(`output = input.parse_jwt_es256(%q)`, dummySecretECDSA256))
	require.NoError(t, err)

	_, err = exe.Query(noneJWT)
	require.ErrorContains(t, err, "incorrect signing method")
}

func TestJwtHSEquivalenceV1V2(t *testing.T) {
	secret := "what-is-love"
	signed := "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.EvUOdbPC4jsI_lN265eoidq7b0HrJSlg-DmmBqV_IyE"

	t.Run("parse_jwt_hs256", func(t *testing.T) {
		migratortest.AssertEquivalent(t,
			fmt.Sprintf(`root = this.parse_jwt_hs256(%q)`, secret),
			signed,
			map[string]any{"sub": "user1338", "name": "Not Blobathan"},
		)
	})
	t.Run("parse_jwt_hs256 from bytes", func(t *testing.T) {
		migratortest.AssertEquivalent(t,
			fmt.Sprintf(`root = this.parse_jwt_hs256(%q)`, secret),
			[]byte(signed),
			map[string]any{"sub": "user1338", "name": "Not Blobathan"},
		)
	})
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
