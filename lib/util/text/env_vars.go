package text

import (
	"bytes"
	"github.com/tidwall/gjson"
	"os"
	"regexp"
	"strings"
)

//------------------------------------------------------------------------------

var (
	envRegex        = regexp.MustCompile(`\${[0-9A-Za-z_.\[\]\\ ]+(:((\${[^}]+})|[^}])+)?}`)
	escapedEnvRegex = regexp.MustCompile(`\${({[0-9A-Za-z_.\[\]\\ ]+(:((\${[^}]+})|[^}])+)?})}`)
	jsonPathRegex   = regexp.MustCompile(`(\[.+])|(\[.+])\.|[.]|(".+")`)
)

// ContainsEnvVariables returns true if inBytes contains environment variable
// replace patterns.
func ContainsEnvVariables(inBytes []byte) bool {
	return envRegex.Find(inBytes) != nil || escapedEnvRegex.Find(inBytes) != nil
}

// TargetVarIsJson returns true if inBytes contains either dots or square brackets
// signalling that the targetVar is a json object
func TargetVarIsJson(inBytes []byte) bool {
	return jsonPathRegex.Find(inBytes) != nil
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
		var defaultVal string
		var targetVar string

		if len(content) < 3 {
			return []byte{}
		}

		if colonIndex := bytes.IndexByte(content, ':'); colonIndex > -1 {
			targetVar = string(content[2:colonIndex])
			defaultVal = string(content[colonIndex+1 : len(content)-1])
		} else {
			targetVar = string(content[2 : len(content)-1])
		}

		if TargetVarIsJson([]byte(targetVar)) {
			targetVarSep := strings.SplitN(targetVar, ".", 2)
			targetVar = targetVarSep[0]
			jsonPath := targetVarSep[1]
			jsonValue := os.Getenv(targetVar)
			value = gjson.Get(jsonValue, jsonPath).String()
		} else {
			value = os.Getenv(targetVar)
		}

		if value == "" {
			value = defaultVal
		}
		// Escape newlines, otherwise there's no way that they would work
		// within a config.
		value = strings.ReplaceAll(value, "\n", "\\n")
		return []byte(value)
	})
	replaced = escapedEnvRegex.ReplaceAll(replaced, []byte("$$$1"))
	return replaced
}

//------------------------------------------------------------------------------
