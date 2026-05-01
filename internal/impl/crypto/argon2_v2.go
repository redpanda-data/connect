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
	"crypto/subtle"

	"golang.org/x/crypto/argon2"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	registerArgon2CompareMethodV2()
}

func registerArgon2CompareMethodV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("String Manipulation").
		Description("Checks whether a string matches a hashed secret using Argon2.").
		Param(bloblangv2.NewStringParam("hashed_secret").Description("The hashed secret to compare with the input. This must be a fully-qualified string which encodes the Argon2 options used to generate the hash."))

	bloblangv2.MustRegisterMethod("compare_argon2", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
		hashedSecret, err := args.GetString("hashed_secret")
		if err != nil {
			return nil, err
		}

		return func(v any) (any, error) {
			source, err := valueAsString(v)
			if err != nil {
				return nil, err
			}
			input := []byte(source)

			if len(input) == 0 {
				return false, nil
			}

			parsedHash, err := decodeArgon2Hash(hashedSecret)
			if err != nil {
				return nil, err
			}

			var hashedInput []byte
			if parsedHash.format == "argon2i" {
				hashedInput = argon2.Key(input, parsedHash.salt, parsedHash.iterations, parsedHash.memory, parsedHash.parallelism, parsedHash.keyLength)
			} else {
				hashedInput = argon2.IDKey(input, parsedHash.salt, parsedHash.iterations, parsedHash.memory, parsedHash.parallelism, parsedHash.keyLength)
			}

			return subtle.ConstantTimeCompare(hashedInput, parsedHash.key) == 1, nil
		}, nil
	})
}
