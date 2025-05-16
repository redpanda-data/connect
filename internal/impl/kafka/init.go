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

package kafka

import (
	_ "embed"

	"github.com/redpanda-data/benthos/v4/public/service"

	// bloblang functions are registered in init functions under this package
	// so ensure they are loaded first
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

//go:embed redpanda_migrator_bundle_input.tmpl.yaml
var redpandaMigratorInputTemplate []byte

//go:embed redpanda_migrator_bundle_output.tmpl.yaml
var redpandaMigratorOutputTemplate []byte

func init() {
	service.MustRegisterTemplateYAML(string(redpandaMigratorInputTemplate))
	service.MustRegisterTemplateYAML(string(redpandaMigratorOutputTemplate))
}
