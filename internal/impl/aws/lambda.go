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

package aws

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"

	"github.com/redpanda-data/connect/v4/internal/serverless"
)

var handler *serverless.Handler

// RunLambda executes Benthos as an AWS Lambda function. Configuration can be
// stored within the environment variable CONNECT_CONFIG.
func RunLambda() {
	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"./redpanda-connect.yaml",
		"/redpanda-connect.yaml",
		"/etc/redpanda-connect/config.yaml",
		"/etc/redpanda-connect.yaml",

		"./connect.yaml",
		"/connect.yaml",
		"/etc/connect/config.yaml",
		"/etc/connect.yaml",

		"./benthos.yaml",
		"./config.yaml",
		"/benthos.yaml",
		"/etc/benthos/config.yaml",
		"/etc/benthos.yaml",
	}
	if path := os.Getenv("BENTHOS_CONFIG_PATH"); path != "" {
		defaultPaths = append([]string{path}, defaultPaths...)
	}
	if path := os.Getenv("CONNECT_CONFIG_PATH"); path != "" {
		defaultPaths = append([]string{path}, defaultPaths...)
	}

	confStr := os.Getenv("BENTHOS_CONFIG")
	if confStr == "" {
		confStr = os.Getenv("CONNECT_CONFIG")
	}

	if confStr == "" {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if confBytes, err := os.ReadFile(path); err == nil {
				confStr = string(confBytes)
				break
			}
		}
	}

	var err error
	if handler, err = serverless.NewHandler(confStr); err != nil {
		fmt.Fprintf(os.Stderr, "Initialisation error: %v\n", err)
		os.Exit(1)
	}

	lambda.Start(handler.Handle)

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	if err = handler.Close(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Shut down error: %v\n", err)
		os.Exit(1)
	}
}
