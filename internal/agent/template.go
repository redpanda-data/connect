/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package agent

import (
	"embed"

	"github.com/redpanda-data/connect/v4/internal/template"
)

//go:embed template/*
var embeddedTemplate embed.FS

// CreateTemplate generates the agent SDK template for RPCN.
func CreateTemplate(dir string, vars map[string]string) error {
	return template.CreateTemplate(embeddedTemplate, dir, template.WithStrippedPrefix("template"), template.WithVariables(vars))
}
