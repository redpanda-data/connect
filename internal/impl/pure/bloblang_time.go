package pure

import (
	"fmt"
	"time"

	"github.com/itchyny/timefmt-go"
	"github.com/rickb777/date/period"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func asDeprecated(s *bloblang.PluginSpec) *bloblang.PluginSpec {
	tmpSpec := *s
	newSpec := &tmpSpec
	newSpec = newSpec.Deprecated()
	return newSpec
}

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	tsRoundSpec := bloblang.NewPluginSpec().
		Beta().
		Static().
		Category(query.MethodCategoryTime).
		Description(`Returns the result of rounding a timestamp to the nearest multiple of the argument duration (nanoseconds). The rounding behavior for halfway values is to round up. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format. The `+"[`ts_parse`](#ts_parse)"+` method can be used in order to parse different timestamp formats.`).
		Param(bloblang.NewInt64Param("duration").Description("A duration measured in nanoseconds to round by.")).
		Version("4.2.0").
		Example("Use the method `parse_duration` to convert a duration string into an integer argument.",
			`root.created_at_hour = this.created_at.ts_round("1h".parse_duration())`,
			[2]string{
				`{"created_at":"2020-08-14T05:54:23Z"}`,
				`{"created_at_hour":"2020-08-14T06:00:00Z"}`,
			})

	tsRoundCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		iDur, err := args.GetInt64("duration")
		if err != nil {
			return nil, err
		}
		dur := time.Duration(iDur)
		return bloblang.TimestampMethod(func(t time.Time) (any, error) {
			return t.Round(dur), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_round", tsRoundSpec, tsRoundCtor); err != nil {
		panic(err)
	}

	tsTZSpec := bloblang.NewPluginSpec().
		Beta().
		Static().
		Category(query.MethodCategoryTime).
		Description(`Returns the result of converting a timestamp to a specified timezone. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format. The `+"[`ts_parse`](#ts_parse)"+` method can be used in order to parse different timestamp formats.`).
		Param(bloblang.NewStringParam("tz").Description(`The timezone to change to. If set to "UTC" then the timezone will be UTC. If set to "Local" then the local timezone will be used. Otherwise, the argument is taken to be a location name corresponding to a file in the IANA Time Zone database, such as "America/New_York".`)).
		Version("4.3.0").
		Example("",
			`root.created_at_utc = this.created_at.ts_tz("UTC")`,
			[2]string{
				`{"created_at":"2021-02-03T17:05:06+01:00"}`,
				`{"created_at_utc":"2021-02-03T16:05:06Z"}`,
			})

	tsTZCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		timezoneStr, err := args.GetString("tz")
		if err != nil {
			return nil, err
		}
		timezone, err := time.LoadLocation(timezoneStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timezone location name: %w", err)
		}
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			return target.In(timezone), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_tz", tsTZSpec, tsTZCtor); err != nil {
		panic(err)
	}

	//--------------------------------------------------------------------------

	parseDurSpec := bloblang.NewPluginSpec().
		Static().
		Category(query.MethodCategoryTime).
		Description(`Attempts to parse a string as a duration and returns an integer of nanoseconds. A duration string is a possibly signed sequence of decimal numbers, each with an optional fraction and a unit suffix, such as "300ms", "-1.5h" or "2h45m". Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".`).
		Example("",
			`root.delay_for_ns = this.delay_for.parse_duration()`,
			[2]string{
				`{"delay_for":"50us"}`,
				`{"delay_for_ns":50000}`,
			},
		).
		Example("",
			`root.delay_for_s = this.delay_for.parse_duration() / 1000000000`,
			[2]string{
				`{"delay_for":"2h"}`,
				`{"delay_for_s":7200}`,
			},
		)

	parseDurCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.StringMethod(func(s string) (any, error) {
			d, err := time.ParseDuration(s)
			if err != nil {
				return nil, err
			}
			return d.Nanoseconds(), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("parse_duration", parseDurSpec, parseDurCtor); err != nil {
		panic(err)
	}

	parseDurISOSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description(`Attempts to parse a string using ISO-8601 rules as a duration and returns an integer of nanoseconds. A duration string is represented by the format "P[n]Y[n]M[n]DT[n]H[n]M[n]S" or "P[n]W". In these representations, the "[n]" is replaced by the value for each of the date and time elements that follow the "[n]". For example, "P3Y6M4DT12H30M5S" represents a duration of "three years, six months, four days, twelve hours, thirty minutes, and five seconds". The last field of the format allows fractions with one decimal place, so "P3.5S" will return 3500000000ns. Any additional decimals will be truncated.`).
		Example("Arbitrary ISO-8601 duration string to nanoseconds:",
			`root.delay_for_ns = this.delay_for.parse_duration_iso8601()`,
			[2]string{
				`{"delay_for":"P3Y6M4DT12H30M5S"}`,
				`{"delay_for_ns":110839937000000000}`,
			},
		).
		Example("Two hours ISO-8601 duration string to seconds:",
			`root.delay_for_s = this.delay_for.parse_duration_iso8601() / 1000000000`,
			[2]string{
				`{"delay_for":"PT2H"}`,
				`{"delay_for_s":7200}`,
			},
		).
		Example("Two and a half seconds ISO-8601 duration string to seconds:",
			`root.delay_for_s = this.delay_for.parse_duration_iso8601() / 1000000000`,
			[2]string{
				`{"delay_for":"PT2.5S"}`,
				`{"delay_for_s":2.5}`,
			},
		)

	parseDurISOCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.StringMethod(func(s string) (any, error) {
			// No need to normalise the output since we need it expressed as nanoseconds.
			d, err := period.Parse(s, false)
			if err != nil {
				return nil, err
			}
			// The conversion is likely imprecise when the period specifies years, months and days.
			// See method documentation for details on precision.
			return d.DurationApprox().Nanoseconds(), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("parse_duration_iso8601", parseDurISOSpec, parseDurISOCtor); err != nil {
		panic(err)
	}

	//--------------------------------------------------------------------------

	parseTSSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description(`Attempts to parse a string as a timestamp following a specified format and outputs a timestamp, which can then be fed into methods such as ` + "[`ts_format`](#ts_format)" + `.

The input format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value. For an alternative way to specify formats check out the ` + "[`ts_strptime`](#ts_strptime)" + ` method.`).
		Param(bloblang.NewStringParam("format").Description("The format of the target string."))

	parseTSSpecDep := asDeprecated(parseTSSpec)

	parseTSSpec = parseTSSpec.
		Example("",
			`root.doc.timestamp = this.doc.timestamp.ts_parse("2006-Jan-02")`,
			[2]string{
				`{"doc":{"timestamp":"2020-Aug-14"}}`,
				`{"doc":{"timestamp":"2020-08-14T00:00:00Z"}}`,
			},
		)

	parseTSCtor := func(deprecated bool) bloblang.MethodConstructorV2 {
		return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			layout, err := args.GetString("format")
			if err != nil {
				return nil, err
			}
			return bloblang.StringMethod(func(s string) (any, error) {
				ut, err := time.Parse(layout, s)
				if err != nil {
					return nil, err
				}
				if deprecated {
					return ut.Format(time.RFC3339Nano), nil
				}
				return ut, nil
			}), nil
		}
	}

	if err := bloblang.RegisterMethodV2("ts_parse", parseTSSpec, parseTSCtor(false)); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("parse_timestamp", parseTSSpecDep, parseTSCtor(true)); err != nil {
		panic(err)
	}

	parseTSStrptimeSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description("Attempts to parse a string as a timestamp following a specified strptime-compatible format and outputs a timestamp, which can then be fed into [`ts_format`](#ts_format).").
		Param(bloblang.NewStringParam("format").Description("The format of the target string."))

	parseTSStrptimeSpecDep := asDeprecated(parseTSStrptimeSpec)

	parseTSStrptimeSpec = parseTSStrptimeSpec.
		Example(
			"The format consists of zero or more conversion specifiers and ordinary characters (except `%`). All ordinary characters are copied to the output string without modification. Each conversion specification begins with a `%` character followed by the character that determines the behaviour of the specifier. Please refer to [man 3 strptime](https://linux.die.net/man/3/strptime) for the list of format specifiers.",
			`root.doc.timestamp = this.doc.timestamp.ts_strptime("%Y-%b-%d")`,
			[2]string{
				`{"doc":{"timestamp":"2020-Aug-14"}}`,
				`{"doc":{"timestamp":"2020-08-14T00:00:00Z"}}`,
			},
		).
		Example(
			"As an extension provided by the underlying formatting library, [itchyny/timefmt-go](https://github.com/itchyny/timefmt-go), the `%f` directive is supported for zero-padded microseconds, which originates from Python. Note that E and O modifier characters are not supported.",
			`root.doc.timestamp = this.doc.timestamp.ts_strptime("%Y-%b-%d %H:%M:%S.%f")`,
			[2]string{
				`{"doc":{"timestamp":"2020-Aug-14 11:50:26.371000"}}`,
				`{"doc":{"timestamp":"2020-08-14T11:50:26.371Z"}}`,
			},
		)

	parseTSStrptimeCtor := func(deprecated bool) bloblang.MethodConstructorV2 {
		return func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			layout, err := args.GetString("format")
			if err != nil {
				return nil, err
			}
			return bloblang.StringMethod(func(s string) (any, error) {
				ut, err := timefmt.Parse(s, layout)
				if err != nil {
					return nil, err
				}
				if deprecated {
					return ut.Format(time.RFC3339Nano), nil
				}
				return ut, nil
			}), nil
		}
	}

	if err := bloblang.RegisterMethodV2("ts_strptime", parseTSStrptimeSpec, parseTSStrptimeCtor(false)); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("parse_timestamp_strptime", parseTSStrptimeSpecDep, parseTSStrptimeCtor(true)); err != nil {
		panic(err)
	}

	//--------------------------------------------------------------------------

	formatTSSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description(`Attempts to format a timestamp value as a string according to a specified format, or RFC 3339 by default. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format.

The output format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value. For an alternative way to specify formats check out the ` + "[`ts_strftime`](#ts_strftime)" + ` method.`).
		Param(bloblang.NewStringParam("format").Description("The output format to use.").Default(time.RFC3339Nano)).
		Param(bloblang.NewStringParam("tz").Description("An optional timezone to use, otherwise the timezone of the input string is used, or in the case of unix timestamps the local timezone is used.").Optional())

	formatTSSpecDep := asDeprecated(formatTSSpec)

	formatTSSpec = formatTSSpec.
		Example("",
			`root.something_at = (this.created_at + 300).ts_format()`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-08-14T11:50:26.371Z"}`,
		).
		Example(
			"An optional string argument can be used in order to specify the output format of the timestamp. The format is defined by showing how the reference time, defined to be Mon Jan 2 15:04:05 -0700 MST 2006, would be displayed if it were the value.",
			`root.something_at = (this.created_at + 300).ts_format("2006-Jan-02 15:04:05")`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-Aug-14 11:50:26"}`,
		).
		Example(
			"A second optional string argument can also be used in order to specify a timezone, otherwise the timezone of the input string is used, or in the case of unix timestamps the local timezone is used.",
			`root.something_at = this.created_at.ts_format(format: "2006-Jan-02 15:04:05", tz: "UTC")`,
			[2]string{
				`{"created_at":1597405526}`,
				`{"something_at":"2020-Aug-14 11:45:26"}`,
			},
			[2]string{
				`{"created_at":"2020-08-14T11:50:26.371Z"}`,
				`{"something_at":"2020-Aug-14 11:50:26"}`,
			},
		).
		Example(
			"And `ts_format` supports up to nanosecond precision with floating point timestamp values.",
			`root.something_at = this.created_at.ts_format("2006-Jan-02 15:04:05.999999", "UTC")`,
			[2]string{
				`{"created_at":1597405526.123456}`,
				`{"something_at":"2020-Aug-14 11:45:26.123456"}`,
			},
			[2]string{
				`{"created_at":"2020-08-14T11:50:26.371Z"}`,
				`{"something_at":"2020-Aug-14 11:50:26.371"}`,
			},
		)

	formatTSCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		layout, err := args.GetString("format")
		if err != nil {
			return nil, err
		}
		var timezone *time.Location
		tzOpt, err := args.GetOptionalString("tz")
		if err != nil {
			return nil, err
		}
		if tzOpt != nil {
			if timezone, err = time.LoadLocation(*tzOpt); err != nil {
				return nil, fmt.Errorf("failed to parse timezone location name: %w", err)
			}
		}
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			if timezone != nil {
				target = target.In(timezone)
			}
			return target.Format(layout), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_format", formatTSSpec, formatTSCtor); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_timestamp", formatTSSpecDep, formatTSCtor); err != nil {
		panic(err)
	}

	formatTSStrftimeSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description("Attempts to format a timestamp value as a string according to a specified strftime-compatible format. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format.").
		Param(bloblang.NewStringParam("format").Description("The output format to use.")).
		Param(bloblang.NewStringParam("tz").Description("An optional timezone to use, otherwise the timezone of the input string is used.").Optional())

	formatTSStrftimeSpecDep := asDeprecated(formatTSStrftimeSpec)

	formatTSStrftimeSpec = formatTSStrftimeSpec.
		Example(
			"The format consists of zero or more conversion specifiers and ordinary characters (except `%`). All ordinary characters are copied to the output string without modification. Each conversion specification begins with `%` character followed by the character that determines the behaviour of the specifier. Please refer to [man 3 strftime](https://linux.die.net/man/3/strftime) for the list of format specifiers.",
			`root.something_at = (this.created_at + 300).ts_strftime("%Y-%b-%d %H:%M:%S")`,
			// `{"created_at":1597405526}`,
			// `{"something_at":"2020-Aug-14 11:50:26"}`,
		).
		Example(
			"A second optional string argument can also be used in order to specify a timezone, otherwise the timezone of the input string is used, or in the case of unix timestamps the local timezone is used.",
			`root.something_at = this.created_at.ts_strftime("%Y-%b-%d %H:%M:%S", "UTC")`,
			[2]string{
				`{"created_at":1597405526}`,
				`{"something_at":"2020-Aug-14 11:45:26"}`,
			},
			[2]string{
				`{"created_at":"2020-08-14T11:50:26.371Z"}`,
				`{"something_at":"2020-Aug-14 11:50:26"}`,
			},
		).
		Example(
			"As an extension provided by the underlying formatting library, [itchyny/timefmt-go](https://github.com/itchyny/timefmt-go), the `%f` directive is supported for zero-padded microseconds, which originates from Python. Note that E and O modifier characters are not supported.",
			`root.something_at = this.created_at.ts_strftime("%Y-%b-%d %H:%M:%S.%f", "UTC")`,
			[2]string{
				`{"created_at":1597405526}`,
				`{"something_at":"2020-Aug-14 11:45:26.000000"}`,
			},
			[2]string{
				`{"created_at":"2020-08-14T11:50:26.371Z"}`,
				`{"something_at":"2020-Aug-14 11:50:26.371000"}`,
			},
		)

	formatTSStrftimeCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		layout, err := args.GetString("format")
		if err != nil {
			return nil, err
		}
		var timezone *time.Location
		tzOpt, err := args.GetOptionalString("tz")
		if err != nil {
			return nil, err
		}
		if tzOpt != nil {
			if timezone, err = time.LoadLocation(*tzOpt); err != nil {
				return nil, fmt.Errorf("failed to parse timezone location name: %w", err)
			}
		}
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			if timezone != nil {
				target = target.In(timezone)
			}
			return timefmt.Format(target, layout), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_strftime", formatTSStrftimeSpec, formatTSStrftimeCtor); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_timestamp_strftime", formatTSStrftimeSpecDep, formatTSStrftimeCtor); err != nil {
		panic(err)
	}

	formatTSUnixSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description("Attempts to format a timestamp value as a unix timestamp. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format. The [`ts_parse`](#ts_parse) method can be used in order to parse different timestamp formats.")

	formatTSUnixSpecDep := asDeprecated(formatTSUnixSpec)

	formatTSUnixSpec = formatTSUnixSpec.
		Example("",
			`root.created_at_unix = this.created_at.ts_unix()`,
			[2]string{
				`{"created_at":"2009-11-10T23:00:00Z"}`,
				`{"created_at_unix":1257894000}`,
			},
		)

	formatTSUnixCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			return target.Unix(), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_unix", formatTSUnixSpec, formatTSUnixCtor); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_timestamp_unix", formatTSUnixSpecDep, formatTSUnixCtor); err != nil {
		panic(err)
	}

	formatTSUnixMilliSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description("Attempts to format a timestamp value as a unix timestamp with millisecond precision. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format. The [`ts_parse`](#ts_parse) method can be used in order to parse different timestamp formats.")

	formatTSUnixMilliSpecDep := asDeprecated(formatTSUnixMilliSpec)

	formatTSUnixMilliSpec = formatTSUnixMilliSpec.
		Example("",
			`root.created_at_unix = this.created_at.ts_unix_milli()`,
			[2]string{
				`{"created_at":"2009-11-10T23:00:00Z"}`,
				`{"created_at_unix":1257894000000}`,
			},
		)

	formatTSUnixMilliCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			return target.UnixMilli(), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_unix_milli", formatTSUnixMilliSpec, formatTSUnixMilliCtor); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_timestamp_unix_milli", formatTSUnixMilliSpecDep, formatTSUnixMilliCtor); err != nil {
		panic(err)
	}

	formatTSUnixMicroSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description("Attempts to format a timestamp value as a unix timestamp with microsecond precision. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format. The [`ts_parse`](#ts_parse) method can be used in order to parse different timestamp formats.")

	formatTSUnixMicroSpecDep := asDeprecated(formatTSUnixMicroSpec)

	formatTSUnixMicroSpec = formatTSUnixMicroSpec.
		Example("",
			`root.created_at_unix = this.created_at.ts_unix_micro()`,
			[2]string{
				`{"created_at":"2009-11-10T23:00:00Z"}`,
				`{"created_at_unix":1257894000000000}`,
			},
		)

	formatTSUnixMicroCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			return target.UnixMicro(), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_unix_micro", formatTSUnixMicroSpec, formatTSUnixMicroCtor); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_timestamp_unix_micro", formatTSUnixMicroSpecDep, formatTSUnixMicroCtor); err != nil {
		panic(err)
	}

	formatTSUnixNanoSpec := bloblang.NewPluginSpec().
		Category(query.MethodCategoryTime).
		Beta().
		Static().
		Description("Attempts to format a timestamp value as a unix timestamp with nanosecond precision. Timestamp values can either be a numerical unix time in seconds (with up to nanosecond precision via decimals), or a string in RFC 3339 format. The [`ts_parse`](#ts_parse) method can be used in order to parse different timestamp formats.")

	formatTSUnixNanoSpecDep := asDeprecated(formatTSUnixNanoSpec)

	formatTSUnixNanoSpec = formatTSUnixNanoSpec.
		Example("",
			`root.created_at_unix = this.created_at.ts_unix_nano()`,
			[2]string{
				`{"created_at":"2009-11-10T23:00:00Z"}`,
				`{"created_at_unix":1257894000000000000}`,
			},
		)

	formatTSUnixNanoCtor := func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return bloblang.TimestampMethod(func(target time.Time) (any, error) {
			return target.UnixNano(), nil
		}), nil
	}

	if err := bloblang.RegisterMethodV2("ts_unix_nano", formatTSUnixNanoSpec, formatTSUnixNanoCtor); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterMethodV2("format_timestamp_unix_nano", formatTSUnixNanoSpecDep, formatTSUnixNanoCtor); err != nil {
		panic(err)
	}
}
