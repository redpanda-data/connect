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

package lang

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/bwmarrin/snowflake"
	"github.com/gosimple/slug"
	"github.com/oklog/ulid/v2"
	"github.com/rivo/uniseg"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	registerSlugV2()
	registerUnicodeSegmentsV2()
	registerFakeV2()
	registerSnowflakeIDV2()
	if err := registerULIDV2(); err != nil {
		panic(err)
	}
}

func registerSlugV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("String Manipulation").
		Description(`Converts a string into a URL-friendly slug by replacing spaces with hyphens, removing special characters, and converting to lowercase. Supports multiple languages for proper transliteration of non-ASCII characters.`).
		Version("4.2.0").
		Param(bloblangv2.NewStringParam("lang").Default("en"))

	bloblangv2.MustRegisterMethod("slug", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
		langOpt, err := args.GetString("lang")
		if err != nil {
			return nil, err
		}
		return func(v any) (any, error) {
			s, err := valueAsString(v)
			if err != nil {
				return nil, err
			}
			return slug.MakeLang(s, langOpt), nil
		}, nil
	})
}

func registerUnicodeSegmentsV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("String Manipulation").
		Description(`Splits text into segments based on Unicode text segmentation rules. Returns an array of strings representing individual graphemes (visual characters), words (including punctuation and whitespace), or sentences. Handles complex Unicode correctly, including emoji with skin tone modifiers and zero-width joiners.`).
		Param(bloblangv2.NewStringParam("segmentation_type").Description("Type of segmentation: \"grapheme\", \"word\", or \"sentence\""))

	bloblangv2.MustRegisterMethod("unicode_segments", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Method, error) {
		segmentType, err := args.GetString("segmentation_type")
		if err != nil {
			return nil, err
		}
		return func(v any) (any, error) {
			s, err := valueAsString(v)
			if err != nil {
				return nil, err
			}
			var next func(str string, state int) (chunk, rest string, newState int)
			switch segmentType {
			case "word":
				next = uniseg.FirstWordInString
			case "sentence":
				next = uniseg.FirstSentenceInString
			case "grapheme":
				next = func(str string, state int) (chunk, rest string, newState int) {
					chunk, rest, _, newState = uniseg.FirstGraphemeClusterInString(str, state)
					return
				}
			default:
				return nil, fmt.Errorf("unknown segmentation type: %s", segmentType)
			}
			parts := []any{}
			state := -1
			var chunk string
			for len(s) > 0 {
				chunk, s, state = next(s, state)
				parts = append(parts, chunk)
			}
			return parts, nil
		}, nil
	})
}

func registerFakeV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("Fake Data Generation").
		Description("Generates realistic fake data for testing and development purposes. Supports a wide variety of data types including personal information, network addresses, dates/times, financial data, and UUIDs. " +
			"Useful for creating mock data, populating test databases, or anonymizing sensitive information.\n\n" +
			"Supported functions: `latitude`, `longitude`, `unix_time`, `date`, `time_string`, `month_name`, `year_string`, `day_of_week`, `day_of_month`, `timestamp`, `century`, `timezone`, `time_period`, " +
			"`email`, `mac_address`, `domain_name`, `url`, `username`, `ipv4`, `ipv6`, `password`, `jwt`, `word`, `sentence`, `paragraph`, " +
			"`cc_type`, `cc_number`, `currency`, `amount_with_currency`, `title_male`, `title_female`, `first_name`, `first_name_male`, " +
			"`first_name_female`, `last_name`, `name`, `gender`, `chinese_first_name`, `chinese_last_name`, `chinese_name`, `phone_number`, " +
			"`toll_free_phone_number`, `e164_phone_number`, `uuid_hyphenated`, `uuid_digit`.").
		Param(bloblangv2.NewStringParam("function").Description("The name of the faker function to use. See description for full list of supported functions.").Default(""))

	bloblangv2.MustRegisterFunction("fake", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Function, error) {
		functionKey, err := args.GetString("function")
		if err != nil {
			return nil, err
		}
		return func() (any, error) {
			return GetFakeValue(functionKey)
		}, nil
	})
}

func registerSnowflakeIDV2() {
	spec := bloblangv2.NewPluginSpec().
		Category("General").
		Description("Generates a unique, time-ordered Snowflake ID. Snowflake IDs are 64-bit integers that encode timestamp, node ID, and sequence information, making them ideal for distributed systems where sortable unique identifiers are needed. Returns a string representation of the ID.").
		Param(bloblangv2.NewInt64Param("node_id").Description("Optional node identifier (0-1023) to distinguish IDs generated by different machines in a distributed system. Defaults to 1.").Default(int64(1)))

	bloblangv2.MustRegisterFunction("snowflake_id", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Function, error) {
		nodeID, err := args.GetInt64("node_id")
		if err != nil {
			return nil, err
		}
		node, err := snowflake.NewNode(nodeID)
		if err != nil {
			return nil, err
		}
		return func() (any, error) {
			return node.Generate().String(), nil
		}, nil
	})
}

func registerULIDV2() error {
	spec := bloblangv2.NewPluginSpec().
		Category("General").
		Description("Generates a Universally Unique Lexicographically Sortable Identifier (ULID). ULIDs are 128-bit identifiers that are sortable by creation time, URL-safe, and case-insensitive. They consist of a 48-bit timestamp (millisecond precision) and 80 bits of randomness, making them ideal for distributed systems that need time-ordered unique IDs without coordination.").
		Param(
			bloblangv2.NewStringParam("encoding").
				Default("crockford").
				Description("Encoding format for the ULID. \"crockford\" produces 26-character Base32 strings (recommended). \"hex\" produces 32-character hexadecimal strings."),
		).
		Param(
			bloblangv2.NewStringParam("random_source").
				Default("secure_random").
				Description("Randomness source: \"secure_random\" uses cryptographically secure random (recommended for production), \"fast_random\" uses faster but non-secure random (only for non-sensitive testing)."),
		)

	secureRandom := rand.Reader
	fastRandom := ulid.DefaultEntropy()

	return bloblangv2.RegisterFunction("ulid", spec, func(args *bloblangv2.ParsedParams) (bloblangv2.Function, error) {
		encoding, err := args.GetString("encoding")
		if err != nil {
			return nil, err
		}

		if !hasMember([]string{"crockford", "hex"}, encoding) {
			return nil, fmt.Errorf("invalid ulid encoding: %s", encoding)
		}

		source, err := args.GetString("random_source")
		if err != nil {
			return nil, err
		}

		if !hasMember([]string{"secure_random", "fast_random"}, source) {
			return nil, fmt.Errorf("invalid randomness source: %s", source)
		}

		var rdr io.Reader
		if source == "fast_random" {
			rdr = fastRandom
		} else {
			rdr = secureRandom
		}

		return func() (any, error) {
			ms := ulid.Now()

			id, err := ulid.New(ms, rdr)
			if err != nil {
				return nil, fmt.Errorf("generating ulid: %s", err)
			}

			switch encoding {
			case "crockford":
				bs, err := id.MarshalText()
				if err != nil {
					return nil, fmt.Errorf("marshalling text: %s", err)
				}
				return string(bs), nil
			case "hex":
				bs, err := id.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("marshalling binary: %s", err)
				}
				return hex.EncodeToString(bs), nil
			default:
				return nil, fmt.Errorf("could not encode ULID with %s", encoding)
			}
		}, nil
	})
}

// valueAsString preserves V1's StringMethod behaviour, which coerced []byte to
// string. V2's StringMethod is strict; preserving lenient coercion here keeps
// existing callers passing string fields (the common case) and bytes (the
// uncommon-but-supported case) working unchanged.
func valueAsString(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	}
	return "", fmt.Errorf("expected string or bytes, got %T", v)
}
