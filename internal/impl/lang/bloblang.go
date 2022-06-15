package lang

import (
	"fmt"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/bxcodec/faker/v3"
	"github.com/gosimple/slug"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	slugSpec := bloblang.NewPluginSpec().
		Experimental().
		Category("String Manipulation").
		Description(`Creates a "slug" from a given string. Wraps the github.com/gosimple/slug package. See its [docs](https://pkg.go.dev/github.com/gosimple/slug) for more information.`).
		Version("4.2.0").
		Example("Creates a slug from an English string",
			`root.slug = this.value.slug()`,
			[2]string{
				`{"value":"Gopher & Benthos"}`,
				`{"slug":"gopher-and-benthos"}`,
			}).
		Example("Creates a slug from a French string",
			`root.slug = this.value.slug("fr")`,
			[2]string{
				`{"value":"Gaufre & Poisson d'Eau Profonde"}`,
				`{"slug":"gaufre-et-poisson-deau-profonde"}`,
			}).Param(bloblang.NewStringParam("lang").Optional().Default("en"))

	if err := bloblang.RegisterMethodV2(
		"slug", slugSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			langOpt, err := args.GetString("lang")
			if err != nil {
				return nil, err
			}
			return bloblang.StringMethod(func(s string) (interface{}, error) {
				return slug.MakeLang(s, langOpt), nil
			}), nil
		},
	); err != nil {
		panic(err)
	}

	fakerSpec := bloblang.NewPluginSpec().
		Experimental().
		Category(query.FunctionCategoryFakeData).
		Description("Takes in a string that maps to a [faker](https://github.com/bxcodec/faker) function and returns the result from that faker function. "+
			"Returns an error if the given string doesn't match a supported faker function").
		Param(bloblang.NewStringParam("function").Description("The name of the function to use to generate the value.").Default("")).

		// Location functions
		Example("Generates a random latitude float value",
			`root.latitude = fake("latitude")`).
		Example("Generates a random longitude float value",
			`root.longitude = fake("longitude")`).

		// Date time functions
		Example("Generates a unix time value",
			`root.unix_time = fake("unix_time")`).
		Example("Generates a date in format YYYY-MM-DD",
			`root.date = fake("date")`).
		Example("Generates a time in the format 00:00:00",
			`root.time = fake("time_string")`).
		Example("Returns a random month name",
			`root.month = fake("month_name")`).
		Example("Returns a year value in string format",
			`root.year = fake("year_string")`).
		Example("Returns a random day of week string (ex: Sunday)",
			`root.day = fake("day_of_week")`).
		Example("Returns a day of month value (ex: 20)",
			`root.day = fake("day_of_month")`).
		Example("Returns a timestamp in the format YYYY-MM-DD hh:mm:ss",
			`root.timestamp = fake("timestamp")`).
		Example("Returns a random century value (ex: IV)",
			`root.century = fake("century")`).
		Example("Returns a random timezone (ex: Asia/Jakarta)",
			`root.timezone = fake("timezone")`).
		Example("Returns either AM or PM",
			`root.time_period = fake("time_period")`).

		// Internet functions
		Example("Generates a string in email address format.",
			`root.email = fake("email")`).
		Example("Generates a string in mac address format.",
			`root.mac_address = fake("mac_address")`).
		Example("Generates a string in domain format (ex: xyz.com)",
			`root.domain = fake("domain_name")`).
		Example("Generates a string in URL format (ex: https://www.xyz.com/abc",
			`root.url = fake("url")`).
		Example("Generates a username string (ex: lVxELHS)",
			`root.username = fake("username")`).
		Example("Generates an IPv4 address",
			`root.ip = fake("ipv4")`).
		Example("Generates an IPv6 address",
			`root.ip = fake("ipv6")`).
		Example("Generates a password string",
			`root.password = fake("password")`).
		Example("Generates a JWT token",
			`root.jwt = fake("jwt")`).

		// Words and sentences functions
		Example("Generates a lorem ipsum word",
			`root.word = fake("word")`).
		Example("Generates a lorem ipsum sentence",
			`root.sentence = fake("sentence")`).
		Example("Generates a lorem ipsum paragraph",
			`root.paragraph = fake("paragraph")`).

		// Payment functions
		Example("Generates a CC type (ex: Visa)",
			`root.cc_type = fake("cc_type")`).
		Example("Generates a CC number",
			`root.cc_number = fake("cc_number")`).
		Example("Generates a currency string (ex: USD)",
			`root.currency = fake("currency")`).
		Example("Generates an amount with a currency label (ex: USD 123.45)",
			`root.currency_value = fake("amount_with_currency")`).

		// Person functions
		Example("Generates a male title (ex: Mr.)",
			`root.title = fake("title_male")`).
		Example("Generates a female title (ex: Mrs.)",
			`root.title = fake("title_female")`).
		Example("Generates a first name",
			`root.first_name = fake("first_name")`).
		Example("Generates a male first name",
			`root.first_name = fake("first_name_male")`).
		Example("Generates a female first name",
			`root.first_name = fake("first_name_female")`).
		Example("Generates a last name",
			`root.last_name = fake("last_name")`).
		Example("Generates a full name with title",
			`root.name = fake("name")`).
		Example("Returns one of the following: 'Male', 'Female', 'Prefer to skip'",
			`root.gender = fake("gender")`).
		Example("Generates a Chinese first name",
			`root.first_name = fake("chinese_first_name")`).
		Example("Generates a Chinese last name",
			`root.last_name = fake("chinese_last_name")`).
		Example("Generates a Chinese full name",
			`root.name = fake("chinese_name")`).

		// Phone functions
		Example("Generates a phone number in format '000-000-0000'",
			`root.phone_number = fake("phone_number")`).
		Example("Generates a toll free phone number in format '(000) 000-000000'",
			`root.phone_number = fake("toll_free_phone_number")`).
		Example("Generates an E164 phone number in the format '+000000000000",
			`root.phone_number = fake("e164_phone_number")`).

		// UUID functions
		Example("Generates a hypenated UUID",
			`root.uuid = fake("uuid_hyphenated")`).
		Example("generates an unhyphenated UUID",
			`root.uuid = fake("uuid_digit")`)

	if err := bloblang.RegisterFunctionV2(
		"fake", fakerSpec,
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			functionKey, err := args.GetString("function")
			if err != nil {
				return nil, err
			}

			return func() (interface{}, error) {
				return GetFakeValue(functionKey)
			}, nil
		},
	); err != nil {
		panic(err)
	}
}

// GetFakeValue returns fake data generated by the faker function corresponding to the input string
func GetFakeValue(function string) (interface{}, error) {
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
