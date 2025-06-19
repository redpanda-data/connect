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
	"errors"

	"golang.org/x/crypto/bcrypt"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func registerCompareBCryptMethod() error {
	spec := bloblang.NewPluginSpec().
		Category("String Manipulation").
		Description("Checks whether a string matches a hashed secret using bcrypt.").
		Param(bloblang.NewStringParam("hashed_secret").Description("The hashed secret value to compare with the input.")).
		Example("", `root.match = this.secret.compare_bcrypt("$2y$10$Dtnt5NNzVtMCOZONT705tOcS8It6krJX8bEjnDJnwxiFKsz1C.3Ay")`, [2]string{
			`{"secret":"there-are-many-blobs-in-the-sea"}`,
			`{"match":true}`,
		}).
		Example("", `root.match = this.secret.compare_bcrypt("$2y$10$Dtnt5NNzVtMCOZONT705tOcS8It6krJX8bEjnDJnwxiFKsz1C.3Ay")`, [2]string{
			`{"secret":"will-i-ever-find-love"}`,
			`{"match":false}`,
		})

	return bloblang.RegisterMethodV2("compare_bcrypt", spec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		hashedSecret, err := args.GetString("hashed_secret")
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(source string) (any, error) {
			input := []byte(source)
			expected := []byte(hashedSecret)

			err := bcrypt.CompareHashAndPassword(expected, input)
			if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
				return false, nil
			}
			if err != nil {
				return nil, err
			}

			return true, nil
		}), nil
	})
}

func init() {
	if err := registerCompareBCryptMethod(); err != nil {
		panic(err)
	}
}
