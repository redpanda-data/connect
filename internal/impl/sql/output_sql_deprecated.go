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

package sql

import (
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func sqlDeprecatedOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Deprecated().
		Categories("Services").
		Summary("Executes an arbitrary SQL query for each message.").
		Description(`
== Alternatives

For basic inserts use the ` + "xref:components:outputs/sql.adoc[`sql_insert`]" + ` output. For more complex queries use the ` + "xref:components:outputs/sql_raw.adoc[`sql_raw`]" + ` output.`).
		Field(driverField).
		Field(service.NewStringField("data_source_name").Description("Data source name.")).
		Field(rawQueryField().
			Example("INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);")).
		Field(service.NewBloblangField("args_mapping").
			Description("An optional xref:guides:bloblang/about.adoc[Bloblang mapping] which should evaluate to an array of values matching in size to the number of placeholder arguments in the field `query`.").
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
	service.MustRegisterBatchOutput(
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
	argsConverter := func(v []any) []any { return v }

	connSettings, err := connSettingsFromParsed(conf, mgr)
	if err != nil {
		return nil, err
	}
	return newSQLRawOutput(
		mgr.Logger(),
		driverStr,
		dsnStr,
		[]rawQueryStatement{{queryStatic, nil, argsMapping, false}},
		argsConverter,
		connSettings), nil
}
