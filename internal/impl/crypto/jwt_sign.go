// Copyright 2024 Redpanda Data, Inc.
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
	"maps"
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
			return nil, fmt.Errorf("decoding signing_secret: %w", err)
		}

		h, err := args.Get("headers")
		if err != nil {
			return nil, err
		}
		var customHeaders map[string]any
		if h != nil {
			switch htype := h.(type) {
			case map[string]any:
				customHeaders = make(map[string]any, len(htype))
				for key, value := range htype {
					if key == "alg" || key == "typ" || key == "jku" || key == "jwk" || key == "x5u" || key == "x5c" || key == "x5t" || key == "x5t#S256" || key == "crit" {
						continue
					}
					customHeaders[key] = value
				}
			default:
				return nil, fmt.Errorf("headers parameter must be an object (map), got %T", h)
			}
		}

		return bloblang.ObjectMethod(func(obj map[string]any) (any, error) {
			token := jwt.NewWithClaims(method, jwt.MapClaims(obj))
			maps.Copy(token.Header, customHeaders)
			signed, err := token.SignedString(s)
			if err != nil {
				return "", fmt.Errorf("signing token: %w", err)
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
		Param(bloblang.NewAnyParam("headers").Optional().Description("Optional object of JWT header fields to include in the token. Keys \"alg\", \"typ\", \"jku\", \"jwk\", \"x5u\", \"x5c\", \"x5t\",\"x5t#S256\" and \"crit\" will be ignored if provided.")).
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

	spec.ExampleNotTested(
		"",
		fmt.Sprintf(`root.signed = this.claims.%s(signing_secret: """%s""", headers: {"kid": "my-key", "x": "y"})`, m.name, m.dummySecret),
		[2]string{
			`{"claims":{"sub":"user123"}}`,
			`{"signed":"<signed JWT token>"}`,
		},
	)

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
			sampleSignature: "<signed JWT token>",
		},
		{
			method:          jwt.SigningMethodHS384,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "<signed JWT token>",
		},
		{
			method:          jwt.SigningMethodHS512,
			dummySecret:     dummySecretHMAC,
			secretDecoder:   hmacSecretDecoder,
			version:         "v4.12.0",
			sampleSignature: "<signed JWT token>",
		},

		{
			method:          jwt.SigningMethodRS256,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaSecretDecoder,
			version:         "v4.18.0",
			sampleSignature: "<signed JWT token>",
		},
		{
			method:          jwt.SigningMethodRS384,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaSecretDecoder,
			version:         "v4.18.0",
			sampleSignature: "<signed JWT token>",
		},
		{
			method:          jwt.SigningMethodRS512,
			dummySecret:     dummySecretRSA,
			secretDecoder:   rsaSecretDecoder,
			version:         "v4.18.0",
			sampleSignature: "<signed JWT token>",
		},

		{
			method:          jwt.SigningMethodES256,
			dummySecret:     dummySecretECDSA,
			secretDecoder:   ecdsaSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "<signed JWT token>",
		},
		{
			method:          jwt.SigningMethodES384,
			dummySecret:     dummySecretECDSA,
			secretDecoder:   ecdsaSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "<signed JWT token>",
		},
		{
			method:          jwt.SigningMethodES512,
			dummySecret:     dummySecretECDSA,
			secretDecoder:   ecdsaSecretDecoder,
			version:         "v4.20.0",
			sampleSignature: "<signed JWT token>",
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
