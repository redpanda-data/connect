package crypto

import (
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

type secretDecoderFunc func(secret string) (any, error)

func hmacSecretDecoder(secret string) (any, error) {
	return []byte(secret), nil
}

func rsaSecretDecoder(secret string) (any, error) {
	return jwt.ParseRSAPrivateKeyFromPEM([]byte(secret))
}

func jwtSigner(secretDecoder secretDecoderFunc, method jwt.SigningMethod) bloblang.MethodConstructorV2 {
	return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		signingSecret, err := args.GetString("signing_secret")
		if err != nil {
			return nil, err
		}
		s, err := secretDecoder(signingSecret)
		if err != nil {
			return nil, fmt.Errorf("failed to decode signing_secret: %w", err)
		}

		return bloblang.ObjectMethod(func(obj map[string]any) (any, error) {
			token := jwt.NewWithClaims(method, jwt.MapClaims(obj))
			signed, err := token.SignedString(s)
			if err != nil {
				return "", fmt.Errorf("failed to sign token: %w", err)
			}

			return signed, nil
		}), nil
	}
}

type signJwtMethodSpec struct {
	name            string
	dummySecret     string
	secretDecoder   secretDecoderFunc
	method          jwt.SigningMethod
	version         string
	sampleSignature string
}

func registerSignJwtMethod(m signJwtMethodSpec) error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryObjectAndArray).
		Description(fmt.Sprintf("Hash and sign an object representing JSON Web Token (JWT) claims using %s.", m.method.Alg())).
		Param(bloblang.NewStringParam("signing_secret").Description("The secret to use for signing the token.")).
		Version(m.version)

	if m.sampleSignature != "" {
		spec.Example(
			"",
			fmt.Sprintf(`root.signed = this.claims.%s("""%s""")`, m.name, m.dummySecret),
			[2]string{
				`{"claims":{"sub":"user123"}}`,
				`{"signed":"` + m.sampleSignature + `"}`,
			},
		)
	}

	return bloblang.RegisterMethodV2(m.name, spec, jwtSigner(m.secretDecoder, m.method))
}

func registerSignJwtMethods() error {
	dummySecretHMAC := "dont-tell-anyone"

	for _, m := range []signJwtMethodSpec{
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
			method:        jwt.SigningMethodRS256,
			secretDecoder: rsaSecretDecoder,
			version:       "v4.18.0",
		},
		{
			method:        jwt.SigningMethodRS384,
			secretDecoder: rsaSecretDecoder,
			version:       "v4.18.0",
		},
		{
			method:        jwt.SigningMethodRS512,
			secretDecoder: rsaSecretDecoder,
			version:       "v4.18.0",
		},
	} {
		m.name = "sign_jwt_" + strings.ToLower(m.method.Alg())
		if err := registerSignJwtMethod(m); err != nil {
			return err
		}
	}

	return nil
}

func init() {
	if err := registerSignJwtMethods(); err != nil {
		panic(err)
	}
}
