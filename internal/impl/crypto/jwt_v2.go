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
	"maps"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	registerParseJwtMethodsV2()
	registerSignJwtMethodsV2()
}

// jwtParserV2 mirrors jwtParser but against the V2 plugin API. The receiver
// coercion preserves V1's StringMethod-leniency (encoded JWT may arrive as
// either string or []byte from the upstream mapping).
func jwtParserV2(secretDecoder secretDecoderFunc, method jwt.SigningMethod) bloblangv2.MethodConstructor {
	return func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
		signingData, err := args.GetString("signing_secret")
		if err != nil {
			return nil, err
		}
		signingSecret, err := secretDecoder(signingData)
		if err != nil {
			return nil, err
		}

		return func(v any) (any, error) {
			encoded, err := valueAsString(v)
			if err != nil {
				return nil, err
			}
			var claims jwt.MapClaims
			_, err = jwt.ParseWithClaims(encoded, &claims, func(tok *jwt.Token) (any, error) {
				if tok.Method != method {
					return nil, fmt.Errorf("%w: %v", errJWTIncorrectMethod, tok.Header["alg"])
				}
				return signingSecret, nil
			})
			if err != nil {
				return nil, fmt.Errorf("parsing JWT string: %w", err)
			}
			return map[string]any(claims), nil
		}, nil
	}
}

// jwtSignerV2 mirrors jwtSigner but against the V2 plugin API. The "headers"
// parameter remains an optional any — paramKindAny is the only optional kind
// that flows nil through V2's coerce path cleanly when omitted via named or
// missing positional args.
func jwtSignerV2(secretDecoder secretDecoderFunc, method jwt.SigningMethod) bloblangv2.MethodConstructor {
	return func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
		signingSecret, err := args.GetString("signing_secret")
		if err != nil {
			return nil, err
		}
		s, err := secretDecoder(signingSecret)
		if err != nil {
			return nil, fmt.Errorf("decoding signing_secret: %w", err)
		}

		var customHeaders map[string]any
		if h, _ := args.Get("headers"); h != nil {
			htype, ok := h.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("headers parameter must be an object (map), got %T", h)
			}
			customHeaders = make(map[string]any, len(htype))
			for key, value := range htype {
				switch key {
				case "alg", "typ", "jku", "jwk", "x5u", "x5c", "x5t", "x5t#S256", "crit":
					continue
				}
				customHeaders[key] = value
			}
		}

		return bloblangv2.ObjectMethod(func(obj map[string]any) (any, error) {
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

func registerParseJwtMethodV2(m parseJwtMethodSpec) {
	spec := bloblangv2.NewPluginSpec().
		Category("JSON Web Tokens").
		Description(fmt.Sprintf("Parses a claims object from a JWT string encoded with %s. This method does not validate JWT claims.", m.method.Alg())).
		Param(bloblangv2.NewStringParam("signing_secret").Description(fmt.Sprintf("The %s secret that was used for signing the token.", m.method.Alg()))).
		Version(m.version)
	bloblangv2.MustRegisterMethod(m.name, spec, jwtParserV2(m.secretDecoder, m.method))
}

func registerSignJwtMethodV2(m signJwtMethodSpec) {
	spec := bloblangv2.NewPluginSpec().
		Category("JSON Web Tokens").
		Description(fmt.Sprintf("Hash and sign an object representing JSON Web Token (JWT) claims using %s.", m.method.Alg())).
		Param(bloblangv2.NewStringParam("signing_secret").Description("The secret to use for signing the token.")).
		Param(bloblangv2.NewAnyParam("headers").Optional().Description("Optional object of JWT header fields to include in the token. Keys \"alg\", \"typ\", \"jku\", \"jwk\", \"x5u\", \"x5c\", \"x5t\",\"x5t#S256\" and \"crit\" will be ignored if provided.")).
		Version(m.version)
	bloblangv2.MustRegisterMethod(m.name, spec, jwtSignerV2(m.secretDecoder, m.method))
}

func registerParseJwtMethodsV2() {
	for _, m := range parseJwtMethodList() {
		m.name = "parse_jwt_" + strings.ToLower(m.method.Alg())
		registerParseJwtMethodV2(m)
	}
}

func registerSignJwtMethodsV2() {
	for _, m := range signJwtMethodList() {
		m.name = "sign_jwt_" + strings.ToLower(m.method.Alg())
		registerSignJwtMethodV2(m)
	}
}

// parseJwtMethodList returns the JWT parser registration data shared by V1 and
// V2. Kept private so it stays an internal implementation detail of the
// connect crypto plugin set.
func parseJwtMethodList() []parseJwtMethodSpec {
	return []parseJwtMethodSpec{
		{method: jwt.SigningMethodHS256, secretDecoder: hmacSecretDecoder, version: "v4.12.0"},
		{method: jwt.SigningMethodHS384, secretDecoder: hmacSecretDecoder, version: "v4.12.0"},
		{method: jwt.SigningMethodHS512, secretDecoder: hmacSecretDecoder, version: "v4.12.0"},
		{method: jwt.SigningMethodRS256, secretDecoder: rsaPublicSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodRS384, secretDecoder: rsaPublicSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodRS512, secretDecoder: rsaPublicSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodES256, secretDecoder: ecdsaPublicSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodES384, secretDecoder: ecdsaPublicSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodES512, secretDecoder: ecdsaPublicSecretDecoder, version: "v4.20.0"},
	}
}

func signJwtMethodList() []signJwtMethodSpec {
	return []signJwtMethodSpec{
		{method: jwt.SigningMethodHS256, secretDecoder: hmacSecretDecoder, version: "v4.12.0"},
		{method: jwt.SigningMethodHS384, secretDecoder: hmacSecretDecoder, version: "v4.12.0"},
		{method: jwt.SigningMethodHS512, secretDecoder: hmacSecretDecoder, version: "v4.12.0"},
		{method: jwt.SigningMethodRS256, secretDecoder: rsaSecretDecoder, version: "v4.18.0"},
		{method: jwt.SigningMethodRS384, secretDecoder: rsaSecretDecoder, version: "v4.18.0"},
		{method: jwt.SigningMethodRS512, secretDecoder: rsaSecretDecoder, version: "v4.18.0"},
		{method: jwt.SigningMethodES256, secretDecoder: ecdsaSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodES384, secretDecoder: ecdsaSecretDecoder, version: "v4.20.0"},
		{method: jwt.SigningMethodES512, secretDecoder: ecdsaSecretDecoder, version: "v4.20.0"},
	}
}
