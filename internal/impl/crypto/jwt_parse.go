package crypto

import (
	"errors"
	"fmt"

	"github.com/golang-jwt/jwt/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

var (
	errJWTUnrecognizedMethod = errors.New("unrecognized signing method")
	errJWTIncorrectMethod    = errors.New("incorrect signing method")
)

func jwtHSParser(alg *jwt.SigningMethodHMAC) bloblang.MethodConstructorV2 {
	return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		signingSecret, err := args.GetString("signing_secret")
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(encoded string) (any, error) {
			var claims jwt.MapClaims

			_, err := jwt.ParseWithClaims(encoded, &claims, func(tok *jwt.Token) (interface{}, error) {
				if _, ok := tok.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("%w: %v", errJWTUnrecognizedMethod, tok.Header["alg"])
				}

				if tok.Method != alg {
					return nil, fmt.Errorf("%w: %v", errJWTIncorrectMethod, tok.Header["alg"])
				}

				return []byte(signingSecret), nil
			})
			if err != nil {
				return nil, fmt.Errorf("failed to parse JWT string: %w", err)
			}

			return map[string]any(claims), nil
		}), nil
	}
}

func jwtRSParser(alg *jwt.SigningMethodRSA) bloblang.MethodConstructorV2 {
	return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		publicKeyString, err := args.GetString("public_key")
		if err != nil {
			return nil, err
		}

		publicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(publicKeyString))
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(encoded string) (any, error) {
			var claims jwt.MapClaims

			_, err := jwt.ParseWithClaims(encoded, &claims, func(tok *jwt.Token) (interface{}, error) {
				if _, ok := tok.Method.(*jwt.SigningMethodRSA); !ok {
					return nil, fmt.Errorf("%w: %v", errJWTUnrecognizedMethod, tok.Header["alg"])
				}

				if tok.Method != alg {
					return nil, fmt.Errorf("%w: %v", errJWTIncorrectMethod, tok.Header["alg"])
				}

				return publicKey, nil
			})
			if err != nil {
				return nil, fmt.Errorf("failed to parse JWT string: %w", err)
			}

			return map[string]any(claims), nil
		}), nil
	}
}

func registerParseJwtHSMethod(name string, signingMethod *jwt.SigningMethodHMAC, docsFixture [2]string) error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryStrings).
		Description(fmt.Sprintf("Parses a claims object from a JWT string encoded with %s. This method does not validate JWT claims.", signingMethod.Alg())).
		Param(bloblang.NewStringParam("signing_secret").Description("The HMAC secret that was used for signing the token.")).
		Example(
			"",
			fmt.Sprintf(`root.claims = this.signed.%s("dont-tell-anyone")`, name),
			docsFixture,
		)

	return bloblang.RegisterMethodV2(name, spec, jwtHSParser(signingMethod))
}

func registerParseJwtRSMethod(name string, signingMethod *jwt.SigningMethodRSA, docsFixture [2]string) error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryStrings).
		Description(fmt.Sprintf("Parses a claims object from a JWT string encoded with %s. This method does not validate JWT claims.", signingMethod.Alg())).
		Param(bloblang.NewStringParam("public_key").Description("The public key that corresponds to the private key used to sign the token.")).
		Example(
			"",
			fmt.Sprintf(`root.claims = this.signed.%s("dont-tell-anyone")`, name),
			docsFixture,
		)

	return bloblang.RegisterMethodV2(name, spec, jwtRSParser(signingMethod))
}

func registerParseJwtHSMethods() error {
	if err := registerParseJwtHSMethod("parse_jwt_hs256", jwt.SigningMethodHS256, [2]string{
		`{"signed":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.hUl-nngPMY_3h9vveWJUPsCcO5PeL6k9hWLnMYeFbFQ"}`,
		`{"claims":{"sub":"user123"}}`,
	}); err != nil {
		return err
	}

	if err := registerParseJwtHSMethod("parse_jwt_hs384", jwt.SigningMethodHS384, [2]string{
		`{"signed":"eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.zGYLr83aToon1efUNq-hw7XgT20lPvZb8sYei8x6S6mpHwb433SJdXJXx0Oio8AZ"}`,
		`{"claims":{"sub":"user123"}}`,
	}); err != nil {
		return err
	}

	if err := registerParseJwtHSMethod("parse_jwt_hs512", jwt.SigningMethodHS512, [2]string{
		`{"signed":"eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.zBNR9o_6EDwXXKkpKLNJhG26j8Dc-mV-YahBwmEdCrmiWt5les8I9rgmNlWIowpq6Yxs4kLNAdFhqoRz3NXT3w"}`,
		`{"claims":{"sub":"user123"}}`,
	}); err != nil {
		return err
	}

	if err := registerParseJwtRSMethod("parse_jwt_rs256", jwt.SigningMethodRS256, [2]string{
		`{"signed":"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.KWin9nTB8d4IZjcCbKQe4jJXc2LfsKKwbSCAMnHcAROpie62Gdjq2m48AEr4EY3iDIdcuqwZoaAwwza_MUvzVDNkjwpdc2ISqYLq9iBczhpG-X3I24Zv28OrCWtZruSM2rl6w7llMSVer35hPjNFPXE_qzIQ7H6O8m3_8tWE1wh2737WdwX0ExjMzYq-bhr5SwYGh905TP521It_YaC6OJ-ijaBR2SgmdriBn7Tov1Qn11iktvOUl-4uRj8Gy-w31O-fZDVklldymdf3uvBByuQkwzl4VkWhr5v2Wvjq49mY4Uj8H-u4NFzrwZtHik56n9YTll0K6k0z3ucUjHpDFA"}`,
		`{"claims":{"sub":"user123"}}`,
	}); err != nil {
		return err
	}

	if err := registerParseJwtRSMethod("parse_jwt_rs384", jwt.SigningMethodRS384, [2]string{
		`{"signed":"eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.detziSnNZJ0cX75pof0EASsajqCmes4otwSYAMjVdr31-gADaGdXTKrkpClUeFdH_488UaekpaeP1iRzML8-kp1yGa6ZCfOw1E_r3zT6hkdZwPDi5OKQy2V5JWlvGTzzwfSc9SgaRGyGg-FBo54CakQMwAA3Us_g82sy4bwO1ay2BriW5dX6tJnm2875DgBzOlHnAt97bH0odT7_LbJPkm9c_H7EdVUH810Qar_NVaPdVgwo5CMN4lCXxIjrFoxCJ3kEu8jf-9bZedK5UHsRlo7lYDxtxrmi9izMXvwCbEcn4Hgi6a_SjsOzsHYriRJN5NCQI_vs4kFiUWiLAyFNeA"}`,
		`{"claims":{"sub":"user123"}}`,
	}); err != nil {
		return err
	}

	if err := registerParseJwtRSMethod("parse_jwt_rs512", jwt.SigningMethodRS512, [2]string{
		`{"signed":"eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTMzOCIsIm5hbWUiOiJOb3QgQmxvYmF0aGFuIn0.eePFKSyF7LHAOehfEKi-V1cOUj5rtHPZ6uyj9VLlihOOyL8jPrny_8w9tsF4YC0jFzsKeRQ2Nnb8_IZqqWhbJgtfUOtkdl4G4CaLEJPUZH3kD_AvVQMsQGjsLO4Mu_rNycLByqk0RZjRVxNTkkt_ArZVSiLX9tmkvvT5fvHTfoGSe56qdhjrzyIcICckwdZU3AJTMf8w3loDISQLEG4OufkrmERXvslAkPN1ZxCZdwg7SHnATz8iEFerGiU-4QNN5dOuQi_XIdPMIbKE6dp4cYDyyr5wVnaEOCDd_TEEenpRLeHsqka3hmQY45rDiOXznpIkpZWeFNmf-4yjVHCZVg"}`,
		`{"claims":{"sub":"user123"}}`,
	}); err != nil {
		return err
	}

	return nil
}

func init() {
	if err := registerParseJwtHSMethods(); err != nil {
		panic(err)
	}
}
