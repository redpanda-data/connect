package crypto

import (
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

type secretDecoderFunc func(secret string) (any, error)

func hmacSecretDecoder(secret string) (any, error) {
	return []byte(secret), nil
}

func rsaSecretDecoder(secret string) (any, error) {
	return jwt.ParseRSAPrivateKeyFromPEM([]byte(secret))
}

func ecdsaSecretDecoder(secret string) (any, error) {
	return jwt.ParseECPrivateKeyFromPEM([]byte(secret))
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
		Category("JSON Web Tokens").
		Description(fmt.Sprintf("Hash and sign an object representing JSON Web Token (JWT) claims using %s.", m.method.Alg())).
		Param(bloblang.NewStringParam("signing_secret").Description("The secret to use for signing the token.")).
		Version(m.version)

	if m.sampleSignature != "" {
		spec.ExampleNotTested(
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
	dummySecretRSA := `-----BEGIN RSA PRIVATE KEY-----
... signature data ...
-----END RSA PRIVATE KEY-----`
	dummySecretECDSA := `-----BEGIN EC PRIVATE KEY-----
... signature data ...
-----END EC PRIVATE KEY-----`

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
			method:          jwt.SigningMethodRS256,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaSecretDecoder,
			version:         "v4.18.0",
			sampleSignature: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.b0lH3jEupZZ4zoaly4Y_GCvu94HH6UKdKY96zfGNsIkPZpQLHIkZ7jMWlLlNOAd8qXlsBGP_i8H2qCKI4zlWJBGyPZgxXDzNRPVrTDfFpn4t4nBcA1WK2-ntXP3ehQxsaHcQU8Z_nsogId7Pme5iJRnoHWEnWtbwz5DLSXL3ZZNnRdrHM9MdI7QSDz9mojKDCaMpGN9sG7Xl-tGdBp1XzXuUOzG8S03mtZ1IgVR1uiBL2N6oohHIAunk8DIAmNWI-zgycTgzUGU7mvPkKH43qO8Ua1-13tCUBKKa8VxcotZ67Mxm1QAvBGoDnTKwWMwghLzs6d6WViXQg6eWlJcpBA",
		},
		{
			method:          jwt.SigningMethodRS384,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaSecretDecoder,
			version:         "v4.18.0",
			sampleSignature: "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.orcXYBcjVE5DU7mvq4KKWFfNdXR4nEY_xupzWoETRpYmQZIozlZnM_nHxEk2dySvpXlAzVm7kgOPK2RFtGlOVaNRIa3x-pMMr-bhZTno4L8Hl4sYxOks3bWtjK7wql4uqUbqThSJB12psAXw2-S-I_FMngOPGIn4jDT9b802ottJSvTpXcy0-eKTjrV2PSkRRu-EYJh0CJZW55MNhqlt6kCGhAXfbhNazN3ASX-dmpd_JixyBKphrngr_zRA-FCn_Xf3QQDA-5INopb4Yp5QiJ7UxVqQEKI80X_JvJqz9WE1qiAw8pq5-xTen1t7zTP-HT1NbbD3kltcNa3G8acmNg",
		},
		{
			method:          jwt.SigningMethodRS512,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaSecretDecoder,
			version:         "v4.18.0",
			sampleSignature: "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.rsMp_X5HMrUqKnZJIxo27aAoscovRA6SSQYR9rq7pifIj0YHXxMyNyOBDGnvVALHKTi25VUGHpfNUW0VVMmae0A4t_ObNU6hVZHguWvetKZZq4FZpW1lgWHCMqgPGwT5_uOqwYCH6r8tJuZT3pqXeL0CY4putb1AN2w6CVp620nh3l8d3XWb4jaifycd_4CEVCqHuWDmohfug4VhmoVKlIXZkYoAQowgHlozATDssBSWdYtv107Wd2AzEoiXPu6e3pflsuXULlyqQnS4ELEKPYThFLafh1NqvZDPddqozcPZ-iODBW-xf3A4DYDdivnMYLrh73AZOGHexxu8ay6nDA",
		},

		{
			method:          jwt.SigningMethodES256,
			dummySecret:     dummySecretECDSA,
			secretDecoder:   ecdsaSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.-8LrOdkEiv_44ADWW08lpbq41ZmHCel58NMORPq1q4Dyw0zFhqDVLrRoSvCvuyyvgXAFb9IHfR-9MlJ_2ShA9A",
		},
		{
			method:          jwt.SigningMethodES384,
			dummySecret:     dummySecretECDSA,
			secretDecoder:   ecdsaSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.8FmTKH08dl7dyxrNu0rmvhegiIBCy-O9cddGco2e9lpZtgv5mS5qHgPkgBC5eRw1d7SRJsHwHZeehzdqT5Ba7aZJIhz9ds0sn37YQ60L7jT0j2gxCzccrt4kECHnUnLw",
		},
		{
			method:          jwt.SigningMethodES512,
			dummySecret:     dummySecretECDSA,
			secretDecoder:   ecdsaSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJFUzUxMiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyMTIzIn0.AQbEWymoRZxDJEJtKSFFG2k2VbDCTYSuBwAZyMqexCspr3If8aERTVGif8HXG3S7TzMBCCzxkcKr3eIU441l3DlpAMNfQbkcOlBqMvNBn-CX481WyKf3K5rFHQ-6wRonz05aIsWAxCDvAozI_9J0OWllxdQ2MBAuTPbPJ38OqXsYkCQs",
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
