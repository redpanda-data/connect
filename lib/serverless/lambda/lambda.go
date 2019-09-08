// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package lambda

import (
	"fmt"
	"os"
	"time"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/serverless"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/aws/aws-lambda-go/lambda"
	"gopkg.in/yaml.v3"
)

var handler *serverless.Handler

// Run executes Benthos as an AWS Lambda function. Configuration can be stored
// within the environment variable BENTHOS_CONFIG.
func Run() {
	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"/benthos.yaml",
		"/etc/benthos/config.yaml",
		"/etc/benthos.yaml",
	}

	conf := config.New()
	conf.Output.Type = serverless.ServerlessResponseType

	if confStr := os.Getenv("BENTHOS_CONFIG"); len(confStr) > 0 {
		confBytes := text.ReplaceEnvVariables([]byte(confStr))
		if err := yaml.Unmarshal(confBytes, &conf); err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if _, err := os.Stat(path); err == nil {
				if _, err = config.Read(path, true, &conf); err != nil {
					fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
					os.Exit(1)
				}
				break
			}
		}
	}

	var err error
	if handler, err = serverless.NewHandler(conf); err != nil {
		fmt.Fprintf(os.Stderr, "Initialisation error: %v\n", err)
		os.Exit(1)
	}

	lambda.Start(handler.Handle)
	if err = handler.Close(time.Second * 30); err != nil {
		fmt.Fprintf(os.Stderr, "Shut down error: %v\n", err)
		os.Exit(1)
	}
}
