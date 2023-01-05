package lambda

import (
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/serverless"
)

var handler *serverless.Handler

// Run executes Benthos as an AWS Lambda function. Configuration can be stored
// within the environment variable BENTHOS_CONFIG.
func Run() {
	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"./benthos.yaml",
		"./config.yaml",
		"/benthos.yaml",
		"/etc/benthos/config.yaml",
		"/etc/benthos.yaml",
	}
	if path := os.Getenv("BENTHOS_CONFIG_PATH"); path != "" {
		defaultPaths = append([]string{path}, defaultPaths...)
	}

	conf := config.New()
	conf.Metrics.Type = "none"
	conf.Logger.Format = "json"

	conf.Output.Type = "switch"
	conf.Output.Switch.RetryUntilSuccess = false

	errorCase := output.NewSwitchConfigCase()
	errorCase.Check = "errored()"
	errorCase.Output.Type = "reject"
	errorCase.Output.Reject = "processing failed due to: ${! error() }"

	responseCase := output.NewSwitchConfigCase()
	responseCase.Output.Type = "sync_response"

	conf.Output.Switch.Cases = append(conf.Output.Switch.Cases, errorCase, responseCase)

	if confStr := os.Getenv("BENTHOS_CONFIG"); len(confStr) > 0 {
		confBytes := config.ReplaceEnvVariables([]byte(confStr))
		if err := yaml.Unmarshal(confBytes, &conf); err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if _, err := ifs.OS().Stat(path); err == nil {
				if _, err = config.ReadFileLinted(ifs.OS(), path, config.LintOptions{}, &conf); err != nil {
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
