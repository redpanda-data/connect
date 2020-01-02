package text

import (
	"bytes"
	"os"
	"regexp"
)

//------------------------------------------------------------------------------

var envRegex *regexp.Regexp
var escapedEnvRegex *regexp.Regexp

func init() {
	var err error
	if envRegex, err = regexp.Compile(`\${[0-9A-Za-z_]+(:((\${[^}]+})|[^}])+)?}`); err != nil {
		panic(err)
	}
	if escapedEnvRegex, err = regexp.Compile(`\${({[0-9A-Za-z_]+(:((\${[^}]+})|[^}])+)?})}`); err != nil {
		panic(err)
	}
}

// ContainsEnvVariables returns true if inBytes contains environment variable
// replace patterns.
func ContainsEnvVariables(inBytes []byte) bool {
	return envRegex.Find(inBytes) != nil || escapedEnvRegex.Find(inBytes) != nil
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
func ReplaceEnvVariables(inBytes []byte) []byte {
	replaced := envRegex.ReplaceAllFunc(inBytes, func(content []byte) []byte {
		var value string
		if len(content) > 3 {
			if colonIndex := bytes.IndexByte(content, ':'); colonIndex == -1 {
				value = os.Getenv(string(content[2 : len(content)-1]))
			} else {
				targetVar := content[2:colonIndex]
				defaultVal := content[colonIndex+1 : len(content)-1]

				value = os.Getenv(string(targetVar))
				if len(value) == 0 {
					value = string(defaultVal)
				}
			}
		}
		return []byte(value)
	})
	replaced = escapedEnvRegex.ReplaceAll(replaced, []byte("$$$1"))
	return replaced
}

//------------------------------------------------------------------------------
