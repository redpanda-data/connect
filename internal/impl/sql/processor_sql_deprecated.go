package sql

import (
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

// DeprecatedProcessorConfig returns a config spec for an sql processor.
func DeprecatedProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Deprecated().
		Categories("Integration").
		Summary("Runs an arbitrary SQL query against a database and (optionally) returns the result as an array of objects, one for each row returned.").
		Description(`
If the query fails to execute then the message will remain unchanged and the error can be caught using error handling methods outlined [here](/docs/configuration/error_handling).

## Alternatives

For basic inserts or select queries use either the ` + "[`sql_insert`](/docs/components/processors/sql_insert)" + ` or the ` + "[`sql_select`](/docs/components/processors/sql_select)" + ` processor. For more complex queries use the ` + "[`sql_raw`](/docs/components/processors/sql_raw)" + ` processor.`).
		Field(driverField).
		Field(service.NewStringField("data_source_name").Description("Data source name.")).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);")).
		Field(service.NewBoolField("unsafe_dynamic_query").
			Description("Whether to enable [interpolation functions](/docs/configuration/interpolation/#bloblang-queries) in the query. Great care should be made to ensure your queries are defended against injection attacks.").
			Advanced().
			Default(false)).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `query`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewStringField("result_codec").
			Description("Result codec.").
			Default("none")).
		Version("3.65.0")
	// TODO: Add example
}

func init() {
	err := service.RegisterBatchProcessor(
		"sql", DeprecatedProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return NewSQLDeprecatedProcessorFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

// NewSQLDeprecatedProcessorFromConfig returns an internal sql processor.
func NewSQLDeprecatedProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawProcessor, error) {
	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}

	dsnStr, err := conf.FieldString("data_source_name")
	if err != nil {
		return nil, err
	}

	queryStatic, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}

	var queryDyn *service.InterpolatedString
	if unsafeDyn, err := conf.FieldBool("unsafe_dynamic_query"); err != nil {
		return nil, err
	} else if unsafeDyn {
		if queryDyn, err = conf.FieldInterpolatedString("query"); err != nil {
			return nil, err
		}
	}

	onlyExec := true
	if codec, err := conf.FieldString("result_codec"); err != nil {
		return nil, err
	} else if codec != "none" {
		onlyExec = false
	}

	var argsMapping *bloblang.Executor
	if conf.Contains("args_mapping") {
		if argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
			return nil, err
		}
	}

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}
	return newSQLRawProcessor(mgr.Logger(), driverStr, dsnStr, queryStatic, queryDyn, onlyExec, argsMapping, connSettings)
}
