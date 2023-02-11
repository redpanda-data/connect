package crypto

import (
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/multierr"
	"golang.org/x/crypto/argon2"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

var (
	errInvalidArgon2Hash = errors.New("invalid argon2 hash")
)

type argon2Value struct {
	format  string
	version string

	salt []byte

	key       []byte
	keyLength uint32

	memory      uint32
	iterations  uint32
	parallelism uint8
}

// decodeArgon2Hash extracts the base64-decoded salt and secret key components
// of an argon2 string as well as the options used for hashing the secret.
func decodeArgon2Hash(hashedSecret string) (*argon2Value, error) {
	// An argon2 string combines the hashing options, salt and key with '$'
	// separators.
	//
	// A sample string looks like this:
	//
	// $argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U
	//
	// The components are:
	//     format:        argon2id
	//     version:       v=19
	//     parameters:    m=4096,t=3,p=1
	//     salt (base64): c2FsdHktbWNzYWx0ZmFjZQ
	//     key (base64):  XTu19IC4rYL/ERsDZr2HOZe9bcMx88ARJ/VVfT2Lb3U

	sep := "$"
	parts := strings.Split(hashedSecret, sep)
	if len(parts) != 6 {
		return nil, errInvalidArgon2Hash
	}

	var value argon2Value

	format := parts[1]
	if format != "argon2i" && format != "argon2id" {
		return nil, fmt.Errorf("%w: unrecognised argon2 format", errInvalidArgon2Hash)
	}

	value.format = format

	_, err := fmt.Sscanf(parts[2], "v=%s", &value.version)
	if err != nil {
		return nil, multierr.Combine(fmt.Errorf("%w: failed to parse version", errInvalidArgon2Hash), err)
	}

	// Parse the hashing parameters segment while disallowing extra trailing
	// characters in the parameters segment of an argon2 string. These can be
	// detected by reintroducing the '$' separator to this segment and ensuring
	// it's the only trailing character consumed by fmt.Sscanf.
	var rest string
	_, err = fmt.Sscanf(parts[3]+sep, "m=%d,t=%d,p=%d%1s", &value.memory, &value.iterations, &value.parallelism, &rest)
	if err != nil {
		return nil, multierr.Combine(fmt.Errorf("%w: failed to parse parameters", errInvalidArgon2Hash), err)
	}
	if rest != sep {
		return nil, fmt.Errorf("%w: excess characters in parameters segment", errInvalidArgon2Hash)
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return nil, multierr.Combine(fmt.Errorf("%w: failed to parse base64 salt", errInvalidArgon2Hash), err)
	}

	value.salt = salt

	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return nil, multierr.Combine(fmt.Errorf("%w: failed to parse base64 key", errInvalidArgon2Hash), err)
	}

	value.key = key

	value.keyLength = uint32(len(key))
	if int(value.keyLength) != len(key) {
		return nil, fmt.Errorf("%w: key length does not fit in uint32", errInvalidArgon2Hash)
	}

	return &value, nil
}

func registerArgon2CompareMethod() error {
	spec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryStrings).
		Description("Checks whether a string matches a hashed secret using Argon2.").
		Param(bloblang.NewStringParam("hashed_secret").Description("The hashed secret to compare with the input. This must be a fully-qualified string which encodes the Argon2 options used to generate the hash.")).
		Example("", `root.match = this.secret.compare_argon2("$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$RMUMwgtS32/mbszd+ke4o4Ej1jFpYiUqY6MHWa69X7Y")`, [2]string{
			`{"secret":"there-are-many-blobs-in-the-sea"}`,
			`{"match":true}`,
		}).
		Example("", `root.match = this.secret.compare_argon2("$argon2id$v=19$m=4096,t=3,p=1$c2FsdHktbWNzYWx0ZmFjZQ$RMUMwgtS32/mbszd+ke4o4Ej1jFpYiUqY6MHWa69X7Y")`, [2]string{
			`{"secret":"will-i-ever-find-love"}`,
			`{"match":false}`,
		})

	return bloblang.RegisterMethodV2("compare_argon2", spec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		hashedSecret, err := args.GetString("hashed_secret")
		if err != nil {
			return nil, err
		}

		return bloblang.StringMethod(func(source string) (interface{}, error) {
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

			match := subtle.ConstantTimeCompare(hashedInput, parsedHash.key) == 1

			return match, nil
		}), nil
	})
}

func init() {
	if err := registerArgon2CompareMethod(); err != nil {
		panic(err)
	}
}
