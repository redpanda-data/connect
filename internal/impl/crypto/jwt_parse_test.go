package crypto

import (
	"fmt"
	"testing"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"

	"crypto/rsa"
)

const dummySecretRSA = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu1SU1LfVLPHCozMxH2Mo
4lgOEePzNm0tRgeLezV6ffAt0gunVTLw7onLRnrq0/IzW7yWR7QkrmBL7jTKEn5u
+qKhbwKfBstIs+bMY2Zkp18gnTxKLxoS2tFczGkPLPgizskuemMghRniWaoLcyeh
kd3qqGElvW/VDL5AaWTg0nLVkjRo9z+40RQzuVaE8AkAFmxZzow3x+VJYKdjykkJ
0iT9wCS0DRTXu269V264Vf/3jvredZiKRkgwlL9xNAwxXFg0x/XFw005UWVRIkdg
cKWTjpBP2dPwVZ4WWC+9aGVd+Gyn1o0CLelf4rEjGoXbAAEgAqeGUxrcIlbjXfbc
mwIDAQAB
-----END PUBLIC KEY-----`

const dummyWrongSecretRSA = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAlN9Fz/vMtd8i4ENuNr/0
Pk5OzPMnoCwctCgK8dKDOObvge8r+bGiAp/fE8aHtUr14Myq6BdKlI4bvp5smfCa
YUVVe1cefOAfEXcDJMcK8KDBck92BwIArPXcXhLyWX+mI8p5pIgeDHM00ABwBNPp
b6sBagFrB66npV7LybptPfX5l0PThPbuHcgNCt7htGGtrXFDT88eRVPyqF/8r/4i
p35NohP5XaiWjeJE2kWs/1fiBNlqirBGCF1QvrpjnIoQqDJSu6QnSPa6yI833LtU
ZQkR/wlCo7zZReU7X9pKmH87+C0a9AiZDOD8HO8eA40kGDofwE1y+Nff7wYiqYlr
rQIDAQAB
-----END PUBLIC KEY-----`

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
	require.ErrorIs(t, err, errJWTIncorrectMethod)
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

func TestBloblangParseJwtRS(t *testing.T) {
	expected := map[string]any{
		"sub":  "user1338",
		"name": "Not Blobathan",
	}

	testCases := []struct {
		method      string
		alg         *jwt.SigningMethodRSA
		signedValue string
	}{
		{
			method: "parse_jwt_rs256", alg: jwt.SigningMethodRS256,
			signedValue: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA",
		},
		{
			method: "parse_jwt_rs384", alg: jwt.SigningMethodRS384,
			signedValue: "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.detziSnNZJ0cX75pof0EASsajqCmes4otwSYAMjVdr31-gADaGdXTKrkpClUeFdH_488UaekpaeP1iRzML8-kp1yGa6ZCfOw1E_r3zT6hkdZwPDi5OKQy2V5JWlvGTzzwfSc9SgaRGyGg-FBo54CakQMwAA3Us_g82sy4bwO1ay2BriW5dX6tJnm2875DgBzOlHnAt97bH0odT7_LbJPkm9c_H7EdVUH810Qar_NVaPdVgwo5CMN4lCXxIjrFoxCJ3kEu8jf-9bZedK5UHsRlo7lYDxtxrmi9izMXvwCbEcn4Hgi6a_SjsOzsHYriRJN5NCQI_vs4kFiUWiLAyFNeA",
		},
		{
			method: "parse_jwt_rs512", alg: jwt.SigningMethodRS512,
			signedValue: "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.eePFKSyF7LHAOehfEKi-V1cOUj5rtHPZ6uyj9VLlihOOyL8jPrny_8w9tsF4YC0jFzsKeRQ2Nnb8_IZqqWhbJgtfUOtkdl4G4CaLEJPUZH3kD_AvVQMsQGjsLO4Mu_rNycLByqk0RZjRVxNTkkt_ArZVSiLX9tmkvvT5fvHTfoGSe56qdhjrzyIcICckwdZU3AJTMf8w3loDISQLEG4OufkrmERXvslAkPN1ZxCZdwg7SHnATz8iEFerGiU-4QNN5dOuQi_XIdPMIbKE6dp4cYDyyr5wVnaEOCDd_TEEenpRLeHsqka3hmQY45rDiOXznpIkpZWeFNmf-4yjVHCZVg",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.method, func(t *testing.T) {
			mapping := fmt.Sprintf("root = this.%s(%q)", tc.method, dummySecretRSA)

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
func TestBloblangParseJwtRS_RejectNoneAlgorithm(t *testing.T) {
	terribleJWT := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0."

	mapping := fmt.Sprintf("root = this.parse_jwt_rs256(%q)", dummySecretRSA)

	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(terribleJWT)
	require.ErrorIs(t, err, errJWTIncorrectMethod)
	require.Nil(t, res)
}

func TestBloblangParseJwtRS_RejectIncorrectHSAlgorithm(t *testing.T) {
	terribleJWT := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA"

	mapping := fmt.Sprintf("root = this.parse_jwt_rs384(%q)", dummySecretRSA)

	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(terribleJWT)
	require.ErrorIs(t, err, errJWTIncorrectMethod)
	require.Nil(t, res)
}

func TestBloblangParseJwtRS_WrongSecret(t *testing.T) {
	terribleJWT := "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA"

	mapping := fmt.Sprintf("root = this.parse_jwt_rs256(%q)", dummyWrongSecretRSA)

	exe, err := bloblang.Parse(mapping)
	require.NoError(t, err)

	res, err := exe.Query(terribleJWT)

	require.ErrorIs(t, err, rsa.ErrVerification)
	require.Nil(t, res)
}
