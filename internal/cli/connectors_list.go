// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"
)

type connectorsList struct {
	Allow []string `yaml:"allow"`
	Deny  []string `yaml:"deny"`
}

// ApplyConnectorsList attempts to read a path (if the file exists) and modifies
// the provided schema according to its contents.
func ApplyConnectorsList(path string, s *service.ConfigSchema) (bool, error) {
	cListBytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to read connector list file: %w", err)
	}

	var cList connectorsList
	if err := yaml.Unmarshal(cListBytes, &cList); err != nil {
		return false, fmt.Errorf("failed to parse connector list file: %w", err)
	}

	if len(cList.Allow) > 0 && len(cList.Deny) > 0 {
		return false, errors.New("connector list must only contain deny or allow items, not both")
	}

	if len(cList.Allow) == 0 && len(cList.Deny) == 0 {
		return false, nil
	}

	env := s.Environment()
	if len(cList.Allow) > 0 {
		env = env.With(cList.Allow...)
	}
	if len(cList.Deny) > 0 {
		env = env.Without(cList.Deny...)
	}

	s.SetEnvironment(env)
	return true, nil
}
