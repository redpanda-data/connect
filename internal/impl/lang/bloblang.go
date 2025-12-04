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

package lang

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bwmarrin/snowflake"
	"github.com/go-faker/faker/v4"
	"github.com/gosimple/slug"
	"github.com/oklog/ulid/v2"
	"github.com/rivo/uniseg"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	slugSpec := bloblang.NewPluginSpec().
		Beta().
		Category("String Manipulation").
		Description(`Converts a string into a URL-friendly slug by replacing spaces with hyphens, removing special characters, and converting to lowercase. Supports multiple languages for proper transliteration of non-ASCII characters.`).
		Version("4.2.0").
		Example("Create a URL-friendly slug from a string with special characters",
			`root.slug = this.title.slug()`,
			[2]string{
				`{"title":"Hello World! Welcome to Redpanda Connect"}`,
				`{"slug":"hello-world-welcome-to-redpanda-connect"}`,
			}).
		Example("Create a slug preserving French language rules",
			`root.slug = this.title.slug("fr")`,
			[2]string{
				`{"title":"Café & Restaurant"}`,
				`{"slug":"cafe-et-restaurant"}`,
			}).Param(bloblang.NewStringParam("lang").Optional().Default("en"))

	if err := bloblang.RegisterMethodV2(
		"slug", slugSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			langOpt, err := args.GetString("lang")
			if err != nil {
				return nil, err
			}
			return bloblang.StringMethod(func(s string) (any, error) {
				return slug.MakeLang(s, langOpt), nil
			}), nil
		},
	); err != nil {
		panic(err)
	}

	unicodeSegmentsSpec := bloblang.NewPluginSpec().
		Beta().
		Category("String Manipulation").
		Description(`Splits text into segments based on Unicode text segmentation rules. Returns an array of strings representing individual graphemes (visual characters), words (including punctuation and whitespace), or sentences. Handles complex Unicode correctly, including emoji with skin tone modifiers and zero-width joiners.`).
		Example("Split text into sentences (preserves trailing spaces)",
			`root.sentences = this.text.unicode_segments("sentence")`,
			[2]string{
				`{"text":"Hello world. How are you?"}`,
				`{"sentences":["Hello world. ","How are you?"]}`,
			}).
		Example("Split text into grapheme clusters (handles complex emoji correctly)",
			`root.graphemes = this.emoji.unicode_segments("grapheme")`,
			[2]string{
				`{"emoji":"👨‍👩‍👧‍👦❤️"}`,
				`{"graphemes":["👨‍👩‍👧‍👦","❤️"]}`,
			}).Param(bloblang.NewStringParam("segmentation_type").Description("Type of segmentation: \"grapheme\", \"word\", or \"sentence\""))

	if err := bloblang.RegisterMethodV2(
		"unicode_segments", unicodeSegmentsSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			segmentType, err := args.GetString("segmentation_type")
			if err != nil {
				return nil, err
			}
			return bloblang.StringMethod(func(s string) (any, error) {
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
			}), nil
		},
	); err != nil {
		panic(err)
	}

	fakerSpec := bloblang.NewPluginSpec().
		Beta().
		Category("Fake Data Generation").
		Description("Generates realistic fake data for testing and development purposes. Supports a wide variety of data types including personal information, network addresses, dates/times, financial data, and UUIDs. "+
			"Useful for creating mock data, populating test databases, or anonymizing sensitive information.\n\n"+
			"Supported functions: `latitude`, `longitude`, `unix_time`, `date`, `time_string`, `month_name`, `year_string`, `day_of_week`, `day_of_month`, `timestamp`, `century`, `timezone`, `time_period`, "+
			"`email`, `mac_address`, `domain_name`, `url`, `username`, `ipv4`, `ipv6`, `password`, `jwt`, `word`, `sentence`, `paragraph`, "+
			"`cc_type`, `cc_number`, `currency`, `amount_with_currency`, `title_male`, `title_female`, `first_name`, `first_name_male`, "+
			"`first_name_female`, `last_name`, `name`, `gender`, `chinese_first_name`, `chinese_last_name`, `chinese_name`, `phone_number`, "+
			"`toll_free_phone_number`, `e164_phone_number`, `uuid_hyphenated`, `uuid_digit`.").
		Param(bloblang.NewStringParam("function").Description("The name of the faker function to use. See description for full list of supported functions.").Default("")).
		Example("Generate fake user profile data for testing",
			`root.user = {
  "id": fake("uuid_hyphenated"),
  "name": fake("name"),
  "email": fake("email"),
  "created_at": fake("timestamp")
}`).
		Example("Create realistic test data for network monitoring",
			`root.event = {
  "source_ip": fake("ipv4"),
  "mac_address": fake("mac_address"),
  "url": fake("url")
}`)

	if err := bloblang.RegisterFunctionV2(
		"fake", fakerSpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			functionKey, err := args.GetString("function")
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				return GetFakeValue(functionKey)
			}, nil
		},
	); err != nil {
		panic(err)
	}

	snowflakeidSpec := bloblang.NewPluginSpec().
		Category("General").
		Description("Generates a unique, time-ordered Snowflake ID. Snowflake IDs are 64-bit integers that encode timestamp, node ID, and sequence information, making them ideal for distributed systems where sortable unique identifiers are needed. Returns a string representation of the ID.").
		Param(bloblang.NewInt64Param("node_id").Description("Optional node identifier (0-1023) to distinguish IDs generated by different machines in a distributed system. Defaults to 1.").Default(int64(1))).
		Example("Generate a unique Snowflake ID for each message",
			`root.id = snowflake_id()
root.payload = this`).
		Example("Generate Snowflake IDs with different node IDs for multi-datacenter deployments",
			`root.id = snowflake_id(42)
root.data = this`)

	if err := bloblang.RegisterFunctionV2(
		"snowflake_id", snowflakeidSpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
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
		},
	); err != nil {
		panic(err)
	}

	if err := registerULID(); err != nil {
		panic(err)
	}
}

// GetFakeValue returns fake data generated by the faker function corresponding to the input string.
func GetFakeValue(function string) (any, error) {
	switch strings.ToLower(function) {
	// Location functions
	case "latitude":
		return faker.Latitude(), nil
	case "longitude":
		return faker.Longitude(), nil

	// Date time functions
	case "unix_time":
		return faker.UnixTime(), nil
	case "date":
		return faker.Date(), nil
	case "time_string":
		return faker.TimeString(), nil
	case "month_name":
		return faker.MonthName(), nil
	case "year_string":
		return faker.YearString(), nil
	case "day_of_week":
		return faker.DayOfWeek(), nil
	case "day_of_month":
		return faker.DayOfMonth(), nil
	case "timestamp":
		return faker.Timestamp(), nil
	case "century":
		return faker.Century(), nil
	case "timezone":
		return faker.Timezone(), nil
	case "time_period":
		return faker.Timeperiod(), nil

	// Internet functions
	case "email":
		return faker.Email(), nil
	case "mac_address":
		return faker.MacAddress(), nil
	case "domain_name":
		return faker.DomainName(), nil
	case "url":
		return faker.URL(), nil
	case "username":
		return faker.Username(), nil
	case "ipv4":
		return faker.IPv4(), nil
	case "ipv6":
		return faker.IPv6(), nil
	case "password":
		return faker.Password(), nil
	case "jwt":
		return faker.Jwt(), nil

	// Words and sentences functions
	case "word":
		return faker.Word(), nil
	case "sentence":
		return faker.Sentence(), nil
	case "paragraph":
		return faker.Paragraph(), nil

	// Payment
	case "cc_type":
		return faker.CCType(), nil
	case "cc_number":
		return faker.CCNumber(), nil
	case "currency":
		return faker.Currency(), nil
	case "amount_with_currency":
		return faker.AmountWithCurrency(), nil

	// Person functions
	case "title_male":
		return faker.TitleMale(), nil
	case "title_female":
		return faker.TitleFemale(), nil
	case "first_name":
		return faker.FirstName(), nil
	case "first_name_male":
		return faker.FirstNameMale(), nil
	case "first_name_female":
		return faker.FirstNameFemale(), nil
	case "last_name":
		return faker.LastName(), nil
	case "name":
		return faker.Name(), nil
	case "gender":
		return faker.Gender(), nil
	case "chinese_first_name":
		return faker.ChineseFirstName(), nil
	case "chinese_last_name":
		return faker.ChineseLastName(), nil
	case "chinese_name":
		return faker.ChineseName(), nil

	// Phone functions
	case "phone_number":
		return faker.Phonenumber(), nil
	case "toll_free_phone_number":
		return faker.TollFreePhoneNumber(), nil
	case "e164_phone_number":
		return faker.E164PhoneNumber(), nil

	// UUID functions
	case "uuid_hyphenated":
		return faker.UUIDHyphenated(), nil
	case "uuid_digit":
		return faker.UUIDDigit(), nil

	case "":
		var str string
		err := faker.FakeData(&str)
		return str, err
	}

	return "", fmt.Errorf("invalid faker function: %s", function)
}

func registerULID() error {
	encodings := []string{"crockford", "hex"}
	randSources := []string{"secure_random", "fast_random"}
	spec := bloblang.NewPluginSpec().
		Experimental().
		Category("General").
		Description("Generates a Universally Unique Lexicographically Sortable Identifier (ULID). ULIDs are 128-bit identifiers that are sortable by creation time, URL-safe, and case-insensitive. They consist of a 48-bit timestamp (millisecond precision) and 80 bits of randomness, making them ideal for distributed systems that need time-ordered unique IDs without coordination.").
		Param(
			bloblang.NewStringParam("encoding").
				Default("crockford").
				Description("Encoding format for the ULID. \"crockford\" produces 26-character Base32 strings (recommended). \"hex\" produces 32-character hexadecimal strings."),
		).
		Param(
			bloblang.NewStringParam("random_source").
				Default("secure_random").
				Description("Randomness source: \"secure_random\" uses cryptographically secure random (recommended for production), \"fast_random\" uses faster but non-secure random (only for non-sensitive testing)."),
		).
		Example(
			"Generate time-sortable IDs for distributed message ordering",
			`root.message_id = ulid()
root.timestamp = now()
root.data = this`,
		).
		Example(
			"Generate hex-encoded ULIDs for systems that prefer hexadecimal format",
			`root.id = ulid("hex")`,
		)

	secureRandom := rand.Reader
	fastRandom := ulid.DefaultEntropy()

	return bloblang.RegisterFunctionV2("ulid", spec, func(args *bloblang.ParsedParams) (bloblang.Function, error) {
		encoding, err := args.GetString("encoding")
		if err != nil {
			return nil, err
		}

		if !hasMember(encodings, encoding) {
			return nil, fmt.Errorf("invalid ulid encoding: %s", encoding)
		}

		source, err := args.GetString("random_source")
		if err != nil {
			return nil, err
		}

		if !hasMember(randSources, source) {
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
				return nil, fmt.Errorf("failed to generate ulid: %s", err)
			}

			switch encoding {
			case "crockford":
				bs, err := id.MarshalText()
				if err != nil {
					return nil, fmt.Errorf("failed to marshal text: %s", err)
				}
				return string(bs), nil
			case "hex":
				bs, err := id.MarshalBinary()
				if err != nil {
					return nil, fmt.Errorf("failed to marshal binary: %s", err)
				}
				return hex.EncodeToString(bs), nil
			default:
				return nil, fmt.Errorf("could not encode ULID with %s", encoding)
			}
		}, nil
	})
}

func hasMember(arr []string, member string) bool {
	return slices.Contains(arr, member)
}
