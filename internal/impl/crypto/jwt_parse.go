package crypto

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

var (
	errJWTIncorrectMethod = errors.New("incorrect signing method")
)

func rsaPublicSecretDecoder(secret string) (any, error) {
	return jwt.ParseRSAPublicKeyFromPEM([]byte(secret))
}

type parseJwtMethodSpec struct {
	name            string
	dummySecret     string
	secretDecoder   secretDecoderFunc
	method          jwt.SigningMethod
	version         string
	sampleSignature string
}

func jwtParser(secretDecoder secretDecoderFunc, method jwt.SigningMethod) bloblang.MethodConstructorV2 {
	return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		signingData, err := args.GetString("signing_secret")
		if err != nil {
			return nil, err
		}
		signingSecret, err := secretDecoder(signingData)
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(encoded string) (any, error) {
			var claims jwt.MapClaims

			_, err := jwt.ParseWithClaims(encoded, &claims, func(tok *jwt.Token) (interface{}, error) {

				if tok.Method != method {
					return nil, fmt.Errorf("%w: %v", errJWTIncorrectMethod, tok.Header["alg"])
				}

				return signingSecret, nil
			})
			if err != nil {
				return nil, fmt.Errorf("failed to parse JWT string: %w", err)
			}

			return map[string]any(claims), nil
		}), nil
	}
}

func registerParseJwtMethod(m parseJwtMethodSpec) error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryStrings).
		Description(fmt.Sprintf("Parses a claims object from a JWT string encoded with %s. This method does not validate JWT claims.", m.method.Alg())).
		Param(bloblang.NewStringParam("signing_secret").Description(fmt.Sprintf("The %s secret that was used for signing the token.", m.method.Alg()))).
		Version(m.version)

	if m.sampleSignature != "" {
		spec.Example(
			"",
			fmt.Sprintf(`root.claims = this.signed.%s("""%s""")`, m.name, m.dummySecret),
			[2]string{
				`{"claims":{"sub":"user123"}}`,
				`{"signed":"` + m.sampleSignature + `"}`,
			},
		)
	}

	return bloblang.RegisterMethodV2(m.name, spec, jwtParser(m.secretDecoder, m.method))
}

func registerParseJwtMethods() error {
	dummySecretHMAC := "dont-tell-anyone"
	dummySecretRSA := `-----BEGIN RSA PUBLIC KEY-----
... certificate data ...
-----END RSA PUBLIC KEY-----`

	for _, m := range []parseJwtMethodSpec{
		{
			method:          jwt.SigningMethodHS256,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.hUl-nngPMY_3h9vveWJUPsCcO5PeL6k9hWLnMYeFbFQ",
		},
		{
			method:          jwt.SigningMethodHS384,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.zGYLr83aToon1efUNq-hw7XgT20lPvZb8sYei8x6S6mpHwb433SJdXJXx0Oio8AZ",
		},
		{
			method:          jwt.SigningMethodHS512,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.zBNR9o_6EDwXXKkpKLNJhG26j8Dc-mV-YahBwmEdCrmiWt5les8I9rgmNlWIowpq6Yxs4kLNAdFhqoRz3NXT3w",
		},
		{
			method:          jwt.SigningMethodRS256,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaPublicSecretDecoder,
			version:         "v4.19.0",
			sampleSignature: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA",
		},
		{
			method:          jwt.SigningMethodRS384,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaPublicSecretDecoder,
			version:         "v4.19.0",
			sampleSignature: "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.detziSnNZJ0cX75pof0EASsajqCmes4otwSYAMjVdr31-gADaGdXTKrkpClUeFdH_488UaekpaeP1iRzML8-kp1yGa6ZCfOw1E_r3zT6hkdZwPDi5OKQy2V5JWlvGTzzwfSc9SgaRGyGg-FBo54CakQMwAA3Us_g82sy4bwO1ay2BriW5dX6tJnm2875DgBzOlHnAt97bH0odT7_LbJPkm9c_H7EdVUH810Qar_NVaPdVgwo5CMN4lCXxIjrFoxCJ3kEu8jf-9bZedK5UHsRlo7lYDxtxrmi9izMXvwCbEcn4Hgi6a_SjsOzsHYriRJN5NCQI_vs4kFiUWiLAyFNeA",
		},
		{
			method:          jwt.SigningMethodRS512,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaPublicSecretDecoder,
			version:         "v4.19.0",
			sampleSignature: "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.eePFKSyF7LHAOehfEKi-V1cOUj5rtHPZ6uyj9VLlihOOyL8jPrny_8w9tsF4YC0jFzsKeRQ2Nnb8_IZqqWhbJgtfUOtkdl4G4CaLEJPUZH3kD_AvVQMsQGjsLO4Mu_rNycLByqk0RZjRVxNTkkt_ArZVSiLX9tmkvvT5fvHTfoGSe56qdhjrzyIcICckwdZU3AJTMf8w3loDISQLEG4OufkrmERXvslAkPN1ZxCZdwg7SHnATz8iEFerGiU-4QNN5dOuQi_XIdPMIbKE6dp4cYDyyr5wVnaEOCDd_TEEenpRLeHsqka3hmQY45rDiOXznpIkpZWeFNmf-4yjVHCZVg",
		},
	} {
		m.name = "parse_jwt_" + strings.ToLower(m.method.Alg())
		if err := registerParseJwtMethod(m); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	if err := registerParseJwtMethods(); err != nil {
		panic(err)
	}
}
