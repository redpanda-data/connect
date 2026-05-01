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
	"errors"

	"golang.org/x/crypto/bcrypt"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	registerCompareBCryptMethodV2()
}

func registerCompareBCryptMethodV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("String Manipulation").
		Description("Checks whether a string matches a hashed secret using bcrypt.").
		Param(bloblangv2.NewStringParam("hashed_secret").Description("The hashed secret value to compare with the input."))

	bloblangv2.MustRegisterMethod("compare_bcrypt", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
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
			expected := []byte(hashedSecret)

			err = bcrypt.CompareHashAndPassword(expected, input)
			if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
				return false, nil
			}
			if err != nil {
				return nil, err
			}
			return true, nil
		}, nil
	})
}
