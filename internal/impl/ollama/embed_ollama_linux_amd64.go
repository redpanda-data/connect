// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build x_benthos_extra

//go:generate curl -LZo ollama https://ollama.com/download/ollama-linux-amd64

package ollama

import _ "embed"

//go:embed ollama
var embeddedServer []byte
