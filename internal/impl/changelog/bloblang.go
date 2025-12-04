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

package changelog

import (
	"github.com/go-viper/mapstructure/v2"
	"github.com/r3labs/diff/v3"
	"go.uber.org/multierr"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	diffSpec := bloblang.NewPluginSpec().
		Beta().
		Category("Object & Array Manipulation").
		Description(`Compares the current value with another value and returns a detailed changelog describing all differences. The changelog contains operations (create, update, delete) with their paths and values, enabling you to track changes between data versions, implement audit logs, or synchronize data between systems.`).
		Version("4.25.0").
		Param(bloblang.NewAnyParam("other").Description("The value to compare against the current value. Can be any structured data (object or array).")).
		Example("Compare two objects to track field changes",
			`root.changes = this.before.diff(this.after)`,
			[2]string{
				`{"before":{"name":"Alice","age":30},"after":{"name":"Alice","age":31,"city":"NYC"}}`,
				`{"changes":[{"From":30,"Path":["age"],"To":31,"Type":"update"},{"From":null,"Path":["city"],"To":"NYC","Type":"create"}]}`,
			}).
		Example("Detect deletions in configuration changes",
			`root.changelog = this.old_config.diff(this.new_config)`,
			[2]string{
				`{"old_config":{"debug":true,"timeout":30},"new_config":{"timeout":60}}`,
				`{"changelog":[{"From":true,"Path":["debug"],"To":null,"Type":"delete"},{"From":30,"Path":["timeout"],"To":60,"Type":"update"}]}`,
			})

	if err := bloblang.RegisterMethodV2("diff", diffSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		other, err := args.Get("other")
		if err != nil {
			return nil, err
		}

		return func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}
			cl, err := diff.Diff(v, other)
			if err != nil {
				return nil, err
			}

			var result []map[string]any
			if err := mapstructure.Decode(cl, &result); err != nil {
				return nil, err
			}

			return result, nil
		}, nil
	}); err != nil {
		panic(err)
	}

	patchSpec := bloblang.NewPluginSpec().
		Beta().
		Category("Object & Array Manipulation").
		Description(`Applies a changelog (created by the diff method) to the current value, transforming it according to the specified operations. This enables you to synchronize data, replay changes, or implement event sourcing patterns by applying recorded changes to reconstruct state.`).
		Version("4.25.0").
		Param(bloblang.NewAnyParam("changelog").Description("The changelog array to apply. Should be in the format returned by the diff method, containing Type, Path, From, and To fields for each change.")).
		Example("Apply recorded changes to update an object",
			`root.updated = this.current.patch(this.changelog)`,
			[2]string{
				`{"current":{"name":"Alice","age":30},"changelog":[{"Type":"update","Path":["age"],"From":30,"To":31},{"Type":"create","Path":["city"],"From":null,"To":"NYC"}]}`,
				`{"updated":{"age":31,"city":"NYC","name":"Alice"}}`,
			}).
		Example("Restore previous state by applying inverse changes",
			`root.restored = this.modified.patch(this.reverse_changelog)`,
			[2]string{
				`{"modified":{"timeout":60},"reverse_changelog":[{"Type":"create","Path":["debug"],"From":null,"To":true},{"Type":"update","Path":["timeout"],"From":60,"To":30}]}`,
				`{"restored":{"debug":true,"timeout":30}}`,
			})

	if err := bloblang.RegisterMethodV2("patch", patchSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		clog, err := args.Get("changelog")
		if err != nil {
			return nil, err
		}

		var cl diff.Changelog
		if err := mapstructure.Decode(clog, &cl); err != nil {
			return nil, err
		}

		return func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}

			pl := diff.Patch(cl, &v)

			if pl.HasErrors() {
				var e error
				for _, ple := range pl {
					if ple.Errors != nil {
						if err := multierr.Append(e, ple.Errors); err != nil {
							return nil, err
						}
					}
				}

				return nil, e
			}

			return v, nil
		}, nil
	}); err != nil {
		panic(err)
	}
}
