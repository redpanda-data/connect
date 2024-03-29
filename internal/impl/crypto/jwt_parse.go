package crypto

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v4"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

var errJWTIncorrectMethod = errors.New("incorrect signing method")

func rsaPublicSecretDecoder(secret string) (any, error) {
	return jwt.ParseRSAPublicKeyFromPEM([]byte(secret))
}

func ecdsaPublicSecretDecoder(secret string) (any, error) {
	return jwt.ParseECPublicKeyFromPEM([]byte(secret))
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
		Category(query.MethodCategoryJWT).
		Description(fmt.Sprintf("Parses a claims object from a JWT string encoded with %s. This method does not validate JWT claims.", m.method.Alg())).
		Param(bloblang.NewStringParam("signing_secret").Description(fmt.Sprintf("The %s secret that was used for signing the token.", m.method.Alg()))).
		Version(m.version)

	if m.sampleSignature != "" {
		spec.Example(
			"",
			fmt.Sprintf(`root.claims = this.signed.%s("""%s""")`, m.name, m.dummySecret),
			[2]string{
				`{"signed":"` + m.sampleSignature + `"}`,
				`{"claims":{"iat":1516239022,"mood":"Disdainful","sub":"1234567890"}}`,
			},
		)
	}

	return bloblang.RegisterMethodV2(m.name, spec, jwtParser(m.secretDecoder, m.method))
}

func registerParseJwtMethods() error {
	dummySecretHMAC := "dont-tell-anyone"
	dummySecretRSA := `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs/ibN8r68pLMR6gRzg4S
8v8l6Q7yi8qURjkEbcNeM1rkokC7xh0I4JVTwxYSVv/JIW8qJdyspl5NIfuAVi32
WfKvSAs+NIs+DMsNPYw3yuQals4AX8hith1YDvYpr8SD44jxhz/DR9lYKZFGhXGB
+7NqQ7vpTWp3BceLYocazWJgusZt7CgecIq57ycM5hjM93BvlrUJ8nQ1a46wfL/8
Cy4P0et70hzZrsjjN41KFhKY0iUwlyU41yEiDHvHDDsTMBxAZosWjSREGfJL6Mfp
XOInTHs/Gg6DZMkbxjQu6L06EdJ+Q/NwglJdAXM7Zo9rNELqRig6DdvG5JesdMsO
+QIDAQAB
-----END PUBLIC KEY-----`

	for _, m := range []parseJwtMethodSpec{
		{
			method:          jwt.SigningMethodHS256,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.YwXOM8v3gHVWcQRRRQc_zDlhmLnM62fwhFYGpiA0J1A",
		},
		{
			method:          jwt.SigningMethodHS384,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.2Y8rf_ijwN4t8hOGGViON_GrirLkCQVbCOuax6EoZ3nluX0tCGezcJxbctlIfsQ2",
		},
		{
			method:          jwt.SigningMethodHS512,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.utRb0urG6LGGyranZJVo5Dk0Fns1QNcSUYPN0TObQ-YzsGGB8jrxHwM5NAJccjJZzKectEUqmmKCaETZvuX4Fg",
		},

		{
			method:          jwt.SigningMethodRS256,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaPublicSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.b0lH3jEupZZ4zoaly4Y_GCvu94HH6UKdKY96zfGNsIkPZpQLHIkZ7jMWlLlNOAd8qXlsBGP_i8H2qCKI4zlWJBGyPZgxXDzNRPVrTDfFpn4t4nBcA1WK2-ntXP3ehQxsaHcQU8Z_nsogId7Pme5iJRnoHWEnWtbwz5DLSXL3ZZNnRdrHM9MdI7QSDz9mojKDCaMpGN9sG7Xl-tGdBp1XzXuUOzG8S03mtZ1IgVR1uiBL2N6oohHIAunk8DIAmNWI-zgycTgzUGU7mvPkKH43qO8Ua1-13tCUBKKa8VxcotZ67Mxm1QAvBGoDnTKwWMwghLzs6d6WViXQg6eWlJcpBA",
		},
		{
			method:          jwt.SigningMethodRS384,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaPublicSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJSUzM4NCIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.orcXYBcjVE5DU7mvq4KKWFfNdXR4nEY_xupzWoETRpYmQZIozlZnM_nHxEk2dySvpXlAzVm7kgOPK2RFtGlOVaNRIa3x-pMMr-bhZTno4L8Hl4sYxOks3bWtjK7wql4uqUbqThSJB12psAXw2-S-I_FMngOPGIn4jDT9b802ottJSvTpXcy0-eKTjrV2PSkRRu-EYJh0CJZW55MNhqlt6kCGhAXfbhNazN3ASX-dmpd_JixyBKphrngr_zRA-FCn_Xf3QQDA-5INopb4Yp5QiJ7UxVqQEKI80X_JvJqz9WE1qiAw8pq5-xTen1t7zTP-HT1NbbD3kltcNa3G8acmNg",
		},
		{
			method:          jwt.SigningMethodRS512,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaPublicSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.rsMp_X5HMrUqKnZJIxo27aAoscovRA6SSQYR9rq7pifIj0YHXxMyNyOBDGnvVALHKTi25VUGHpfNUW0VVMmae0A4t_ObNU6hVZHguWvetKZZq4FZpW1lgWHCMqgPGwT5_uOqwYCH6r8tJuZT3pqXeL0CY4putb1AN2w6CVp620nh3l8d3XWb4jaifycd_4CEVCqHuWDmohfug4VhmoVKlIXZkYoAQowgHlozATDssBSWdYtv107Wd2AzEoiXPu6e3pflsuXULlyqQnS4ELEKPYThFLafh1NqvZDPddqozcPZ-iODBW-xf3A4DYDdivnMYLrh73AZOGHexxu8ay6nDA",
		},

		{
			method: jwt.SigningMethodES256,
			dummySecret: `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGtLqIBePHmIhQcf0JLgc+F/4W/oI
dp0Gta53G35VerNDgUUXmp78J2kfh4qLdh0XtmOMI587tCaqjvDAXfs//w==
-----END PUBLIC KEY-----`,
			secretDecoder:   ecdsaPublicSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.GIRajP9JJbpTlqSCdNEz4qpQkRvzX4Q51YnTwVyxLDM9tKjR_a8ggHWn9CWj7KG0x8J56OWtmUxn112SRTZVhQ",
		},
		{
			method: jwt.SigningMethodES384,
			dummySecret: `-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAERoz74/B6SwmLhs8X7CWhnrWyRrB13AuU
8OYeqy0qHRu9JWNw8NIavqpTmu6XPT4xcFanYjq8FbeuM11eq06C52mNmS4LLwzA
2imlFEgn85bvJoC3bnkuq4mQjwt9VxdH
-----END PUBLIC KEY-----`,
			secretDecoder:   ecdsaPublicSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.H2HBSlrvQBaov2tdreGonbBexxtQB-xzaPL4-tNQZ6TVh7VH8VBcSwcWHYa1lBAHmdsKOFcB2Wk0SB7QWeGT3ptSgr-_EhDMaZ8bA5spgdpq5DsKfaKHrd7DbbQlmxNq",
		},
		{
			method: jwt.SigningMethodES512,
			dummySecret: `-----BEGIN PUBLIC KEY-----
MIGbMBAGByqGSM49AgEGBSuBBAAjA4GGAAQAkHLdts9P56fFkyhpYQ31M/Stwt3w
vpaxhlfudxnXgTO1IP4RQRgryRxZ19EUzhvWDcG3GQIckoNMY5PelsnCGnIBT2Xh
9NQkjWF5K6xS4upFsbGSAwQ+GIyyk5IPJ2LHgOyMSCVh5gRZXV3CZLzXujx/umC9
UeYyTt05zRRWuD+p5bY=
-----END PUBLIC KEY-----`,
			secretDecoder:   ecdsaPublicSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "eyJhbGciOiJFUzUxMiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1MTYyMzkwMjIsIm1vb2QiOiJEaXNkYWluZnVsIiwic3ViIjoiMTIzNDU2Nzg5MCJ9.ACrpLuU7TKpAnncDCpN9m85nkL55MJ45NFOBl6-nEXmNT1eIxWjiP4pwWVbFH9et_BgN14119jbL_KqEJInPYc9nAXC6dDLq0aBU-dalvNl4-O5YWpP43-Y-TBGAsWnbMTrchILJ4-AEiICe73Ck5yWPleKg9c3LtkEFWfGs7BoPRguZ",
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
