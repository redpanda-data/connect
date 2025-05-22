/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package plugin

import "github.com/redpanda-data/benthos/v4/public/service"

type Config struct {
}

func (c *Config) ToSpec() (*service.ConfigSpec, error) {
	return nil, nil
}
