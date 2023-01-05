package sql

import (
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func sqlDeprecatedOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Deprecated().
		Categories("Services").
		Summary("Executes an arbitrary SQL query for each message.").
		Description(`
## Alternatives

For basic inserts use the ` + "[`sql_insert`](/docs/components/outputs/sql)" + ` output. For more complex queries use the ` + "[`sql_raw`](/docs/components/outputs/sql_raw)" + ` output.`).
		Field(driverField).
		Field(service.NewStringField("data_source_name").Description("Data source name.")).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);")).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `query`.").
			Example("root = [ this.cat.meow, this.doc.woofs[0] ]").
			Example(`root = [ meta("user.id") ]`).
			Optional()).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of inserts to run in parallel.").
			Default(64)).
		Field(service.NewBatchPolicyField("batching")).
		Version("3.65.0")
}

func init() {
	err := service.RegisterBatchOutput(
		"sql", sqlDeprecatedOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			out, err = newSQLDeprecatedOutputFromConfig(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

func newSQLDeprecatedOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*sqlRawOutput, error) {
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
	return newSQLRawOutput(mgr.Logger(), driverStr, dsnStr, queryStatic, nil, argsMapping, connSettings), nil
}
