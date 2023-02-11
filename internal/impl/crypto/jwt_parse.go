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

	return nil
}

func init() {
	if err := registerParseJwtHSMethods(); err != nil {
		panic(err)
	}
}
