package lang

import (
	"fmt"
	"github.com/bxcodec/faker/v3"
	"github.com/gosimple/slug"
	"strings"

	"github.com/benthosdev/benthos/v4/public/bloblang"
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
		Category("General").
		Description("Takes in a string that maps to a faker function and returns the result from that faker function. "+
			"Returns an error if the given string doesn't match a supported faker function").
		Param(bloblang.NewStringParam("function").Description("The name of the function to use to generate the value.")).

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
	var result interface{}
	result = ""
	var err error
	err = nil

	switch strings.ToLower(function) {

	// Location functions
	case "latitude":
		result = faker.Latitude()
	case "longitude":
		result = faker.Longitude()

	// Date time functions
	case "unix_time":
		result = faker.UnixTime()
	case "date":
		result = faker.Date()
	case "time_string":
		result = faker.TimeString()
	case "month_name":
		result = faker.MonthName()
	case "year_string":
		result = faker.YearString()
	case "day_of_week":
		result = faker.DayOfWeek()
	case "day_of_month":
		result = faker.DayOfMonth()
	case "timestamp":
		result = faker.Timestamp()
	case "century":
		result = faker.Century()
	case "timezone":
		result = faker.Timezone()
	case "time_period":
		result = faker.Timeperiod()

	// Internet functions
	case "email":
		result = faker.Email()
	case "mac_address":
		result = faker.MacAddress()
	case "domain_name":
		result = faker.DomainName()
	case "url":
		result = faker.URL()
	case "username":
		result = faker.Username()
	case "ipv4":
		result = faker.IPv4()
	case "ipv6":
		result = faker.IPv6()
	case "password":
		result = faker.Password()

	// Words and sentences functions
	case "word":
		result = faker.Word()
	case "sentence":
		result = faker.Sentence()
	case "paragraph":
		result = faker.Paragraph()

	// Payment
	case "cc_type":
		result = faker.CCType()
	case "cc_number":
		result = faker.CCNumber()
	case "currency":
		result = faker.Currency()
	case "amount_with_currency":
		result = faker.AmountWithCurrency()

	// Person functions
	case "title_male":
		result = faker.TitleMale()
	case "title_female":
		result = faker.TitleFemale()
	case "first_name":
		result = faker.FirstName()
	case "first_name_male":
		result = faker.FirstNameMale()
	case "first_name_female":
		result = faker.FirstNameFemale()
	case "last_name":
		result = faker.LastName()
	case "name":
		result = faker.Name()

	// Phone functions
	case "phone_number":
		result = faker.Phonenumber()
	case "toll_free_phone_number":
		result = faker.TollFreePhoneNumber()
	case "e164_phone_number":
		result = faker.E164PhoneNumber()

	// UUID functions
	case "uuid_hyphenated":
		result = faker.UUIDHyphenated()
	case "uuid_digit":
		result = faker.UUIDDigit()
	default:
		err = fmt.Errorf("invalid faker function: %s", function)
	}

	return result, err
}
