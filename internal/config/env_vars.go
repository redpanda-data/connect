package config

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
)

var (
	envRegex        = regexp.MustCompile(`\${[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?}`)
	escapedEnvRegex = regexp.MustCompile(`\${({[0-9A-Za-z_.]+(:((\${[^}]+})|[^}])*)?})}`)
)

// ErrMissingEnvVars is returned when attempting environment variable
// interpolations where the referenced environment variables are missing.
type ErrMissingEnvVars struct {
	Variables []string

	// Our best attempt at parsing the config that's missing variables by simply
	// inserting an empty string. There's a good chance this is still a valid
	// config! :)
	BestAttempt []byte
}

// Error returns a rather sweet error message.
func (e *ErrMissingEnvVars) Error() string {
	// TODO: Deduplicate the variables as they might be repeated.
	return fmt.Sprintf("required environment variables were not set: %v", e.Variables)
}

// ReplaceEnvVariables will search a blob of data for the pattern `${FOO:bar}`,
// where `FOO` is an environment variable name and `bar` is a default value. The
// `bar` section (including the colon) can be left out if there is no
// appropriate default value for the field.
//
// For each aforementioned pattern found in the blob the contents of the
// respective environment variable will be read and will replace the pattern. If
// the environment variable is empty or does not exist then either the default
// value is used or the field will be left empty.
func ReplaceEnvVariables(inBytes []byte, lookupFn func(string) (string, bool)) (replaced []byte, err error) {
	var missingVarsErr ErrMissingEnvVars

	replaced = envRegex.ReplaceAllFunc(inBytes, func(content []byte) []byte {
		var value string
		var ok bool
		if len(content) > 3 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				varName := string(content[2 : len(content)-1])
				if value, ok = lookupFn(varName); !ok {
					missingVarsErr.Variables = append(missingVarsErr.Variables, varName)
				}
			} else {
				targetVar := content[2:colonIndex]
				defaultVal := content[colonIndex+1 : len(content)-1]
				value, _ = lookupFn(string(targetVar))
				if value == "" {
					value = string(defaultVal)
				}
			}
			// Escape newlines, otherwise there's no way that they would work
			// within a config.
			value = strings.ReplaceAll(value, "\n", "\\n")
		}
		return []byte(value)
	})
	replaced = escapedEnvRegex.ReplaceAll(replaced, []byte("$$$1"))

	if len(missingVarsErr.Variables) > 0 {
		missingVarsErr.BestAttempt = replaced
		err = &missingVarsErr
		replaced = nil
	}
	return
}
