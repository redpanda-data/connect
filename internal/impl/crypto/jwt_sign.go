package crypto

import (
	"fmt"

	"github.com/golang-jwt/jwt/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func jwtHSSigner(alg *jwt.SigningMethodHMAC) bloblang.MethodConstructorV2 {
	return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		signingSecret, err := args.GetString("signing_secret")
		if err != nil {
			return nil, err
		}

		return bloblang.ObjectMethod(func(obj map[string]any) (any, error) {
			token := jwt.NewWithClaims(alg, jwt.MapClaims(obj))
			signed, err := token.SignedString([]byte(signingSecret))
			if err != nil {
				return "", fmt.Errorf("failed to sign token: %w", err)
			}

			return signed, nil
		}), nil
	}
}

func registerSignJwtHSMethod(name string, signingMethod *jwt.SigningMethodHMAC, docsFixture [2]string) error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryObjectAndArray).
		Description(fmt.Sprintf("Hash and sign an object representing JSON Web Token (JWT) claims using %s.", signingMethod.Alg())).
		Param(bloblang.NewStringParam("signing_secret").Description("The HMAC secret to use for signing the token.")).
		Example(
			"",
			fmt.Sprintf(`root.signed = this.claims.%s("dont-tell-anyone")`, name),
			docsFixture,
		)

	return bloblang.RegisterMethodV2(name, spec, jwtHSSigner(signingMethod))

}

func registerSignJwtHSMethods() error {
	if err := registerSignJwtHSMethod("sign_jwt_hs256", jwt.SigningMethodHS256, [2]string{
		`{"claims":{"sub":"user123"}}`,
		`{"signed":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.hUl-nngPMY_3h9vveWJUPsCcO5PeL6k9hWLnMYeFbFQ"}`,
	}); err != nil {
		return err
	}

	if err := registerSignJwtHSMethod("sign_jwt_hs384", jwt.SigningMethodHS384, [2]string{
		`{"claims":{"sub":"user123"}}`,
		`{"signed":"eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.zGYLr83aToon1efUNq-hw7XgT20lPvZb8sYei8x6S6mpHwb433SJdXJXx0Oio8AZ"}`,
	}); err != nil {
		return err
	}

	if err := registerSignJwtHSMethod("sign_jwt_hs512", jwt.SigningMethodHS512, [2]string{
		`{"claims":{"sub":"user123"}}`,
		`{"signed":"eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.zBNR9o_6EDwXXKkpKLNJhG26j8Dc-mV-YahBwmEdCrmiWt5les8I9rgmNlWIowpq6Yxs4kLNAdFhqoRz3NXT3w"}`,
	}); err != nil {
		return err
	}

	return nil
}

func init() {
	if err := registerSignJwtHSMethods(); err != nil {
		panic(err)
	}
}
