// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package schema

import (
	"strings"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"

	_ "embed"
)

//go:embed cloud_allow_list.txt
var cloudAllowList string

//go:embed ai_allow_list.txt
var aiAllowList string

func redpandaTopLevelConfigField() *service.ConfigField {
	return service.NewObjectField("redpanda", enterprise.TopicLoggerFields()...)
}

// Standard returns the config schema of a standard build of Redpanda Connect.
func Standard(version, dateBuilt string) *service.ConfigSchema {
	env := service.NewEnvironment()

	s := env.FullConfigSchema(version, dateBuilt)
	s.SetFieldDefault(map[string]any{
		"@service": "redpanda-connect",
	}, "logger", "static_fields")
	s = s.Field(redpandaTopLevelConfigField())
	return s
}

// Cloud returns the config schema of a cloud build of Redpanda Connect.
func Cloud(version, dateBuilt string, aiEnabled bool) *service.ConfigSchema {
	allowList := cloudAllowList
	if aiEnabled {
		allowList = aiAllowList
	}
	var allowSlice []string
	for _, s := range strings.Split(allowList, "\n") {
		s = strings.TrimSpace(s)
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		allowSlice = append(allowSlice, s)
	}

	// Observability and scanner plugins aren't necessarily present in our
	// internal lists and so we allow everything that's imported
	env := service.GlobalEnvironment().
		WithBuffers(allowSlice...).
		WithCaches(allowSlice...).
		WithInputs(allowSlice...).
		WithOutputs(allowSlice...).
		WithProcessors(allowSlice...).
		WithRateLimits(allowSlice...)

	// Allow only pure methods and functions within Bloblang.
	benv := bloblang.GlobalEnvironment()
	env.UseBloblangEnvironment(benv.OnlyPure())

	s := env.FullConfigSchema(version, dateBuilt)
	s.SetFieldDefault(map[string]any{}, "input")
	s.SetFieldDefault(map[string]any{}, "output")
	s.SetFieldDefault(map[string]any{
		"@service": "redpanda-connect",
	}, "logger", "static_fields")
	s = s.Field(redpandaTopLevelConfigField())
	return s
}
