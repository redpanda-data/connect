// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise

import (
	_ "embed"

	"github.com/redpanda-data/benthos/v4/public/service"

	// bloblang functions are registered in init functions under this package
	// so ensure they are loaded first
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

//go:embed kafka_migrator_bundle_input.tmpl.yaml
var kafkaMigratorInputTemplate []byte

//go:embed kafka_migrator_bundle_output.tmpl.yaml
var kafkaMigratorOutputTemplate []byte

func init() {
	if err := service.RegisterTemplateYAML(string(kafkaMigratorInputTemplate)); err != nil {
		panic(err)
	}

	if err := service.RegisterTemplateYAML(string(kafkaMigratorOutputTemplate)); err != nil {
		panic(err)
	}
}
