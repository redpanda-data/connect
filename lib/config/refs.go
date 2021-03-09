package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Jeffail/benthos/v3/lib/util/text"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// ReadWithJSONPointers takes a config file path, reads the contents, performs a
// generic parse, resolves any JSON Pointers, marshals the result back into
// bytes and returns it so that it can be unmarshalled into a typed structure.
func ReadWithJSONPointers(path string, replaceEnvs bool) ([]byte, error) {
	b, _, err := ReadWithJSONPointersLinted(path, replaceEnvs)
	return b, err
}

// ReadWithJSONPointersLinted takes a config file path, reads the contents,
// performs a generic parse, resolves any JSON Pointers, marshals the result
// back into bytes and returns it so that it can be unmarshalled into a typed
// structure.
//
// If any non-fatal errors occur lints are returned along with the result.
func ReadWithJSONPointersLinted(path string, replaceEnvs bool) ([]byte, []string, error) {
	configBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	var lints []string
	if !utf8.Valid(configBytes) {
		lints = append(lints, "Detected invalid utf-8 encoding in config, this may result in interpolation functions not working as expected")
	}

	if replaceEnvs {
		configBytes = text.ReplaceEnvVariables(configBytes)
	}

	var gen interface{}
	if err := yaml.Unmarshal(configBytes, &gen); err != nil {
		return nil, lints, err
	}

	refFound, err := refWalk(path, 0, gen, gen)
	if err != nil {
		return nil, lints, err
	}
	if !refFound {
		return configBytes, lints, nil
	}
	if configBytes, err = yaml.Marshal(gen); err != nil {
		return nil, lints, fmt.Errorf("failed to marshal ref evaluated structure: %v", err)
	}

	return configBytes, lints, nil
}

//------------------------------------------------------------------------------

// JSONPointer parses a JSON pointer path (https://tools.ietf.org/html/rfc6901)
// and either returns an interface{} containing the result or an error if the
// referenced item could not be found.
func JSONPointer(path string, object interface{}) (interface{}, error) {
	if len(path) < 1 {
		return nil, errors.New("failed to resolve JSON pointer: path must not be empty")
	}
	if path[0] != '/' {
		return nil, errors.New("failed to resolve JSON pointer: path must begin with '/'")
	}
	hierarchy := strings.Split(path, "/")[1:]
	for i, v := range hierarchy {
		v = strings.ReplaceAll(v, "~1", "/")
		v = strings.ReplaceAll(v, "~0", "~")
		hierarchy[i] = v
	}

	for target := 0; target < len(hierarchy); target++ {
		pathSeg := hierarchy[target]
		switch typedObject := object.(type) {
		case map[string]interface{}:
			var ok bool
			if object, ok = typedObject[pathSeg]; !ok {
				return nil, fmt.Errorf("failed to resolve JSON pointer: index '%v' value '%v' was not found", target, pathSeg)
			}
		case map[interface{}]interface{}:
			var ok bool
			if object, ok = typedObject[pathSeg]; !ok {
				return nil, fmt.Errorf("failed to resolve JSON pointer: index '%v' value '%v' was not found", target, pathSeg)
			}
		case []interface{}:
			index, err := strconv.Atoi(pathSeg)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve JSON pointer: could not parse index '%v' value '%v' into array index: %v", target, pathSeg, err)
			}
			if len(typedObject) <= index {
				return nil, fmt.Errorf("failed to resolve JSON pointer: index '%v' value '%v' exceeded target array size of '%v'", target, pathSeg, len(typedObject))
			}
			object = typedObject[index]
		default:
			return nil, fmt.Errorf("failed to resolve JSON pointer: index '%v' field '%v' was not found", target, pathSeg)
		}
	}
	return object, nil
}

//------------------------------------------------------------------------------

// ErrExceededRefLimit is returned when a configuration hierarchy results in
// nested references beyond a hard coded limit. This is intended to prevent
// looped references from blocking the service.
var ErrExceededRefLimit = errors.New("exceeded limit of nested references")
var refLimit = 1000

func getRefVal(obj interface{}) interface{} {
	switch x := obj.(type) {
	case map[interface{}]interface{}:
		for k, v := range x {
			if k == "$ref" {
				return v
			}
		}
	case map[string]interface{}:
		for k, v := range x {
			if k == "$ref" {
				return v
			}
		}
	}
	return nil
}

func expandRefVal(path string, level int, root, v interface{}) (interface{}, error) {
	if level == refLimit {
		return nil, ErrExceededRefLimit
	}

	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("config '%v' contained non-string $ref value '%v' (%T)", path, v, v)
	}
	u, err := url.Parse(s)
	if err != nil {
		return nil, fmt.Errorf("failed to parse $ref value '%v' in config '%v' as URI: %v", s, path, err)
	}
	if u.Scheme != "" && u.Scheme != "file" {
		return nil, fmt.Errorf("config '%v' contained non-path $ref value '%v'", path, v)
	}
	if len(u.Path) == 0 && len(u.Fragment) == 0 {
		return nil, fmt.Errorf("failed to resolve $ref value '%v' in config '%v': reference URI must contain at least a path or fragment", s, path)
	}

	var nextObj interface{}
	if len(u.Path) > 0 {
		rPath := u.Path
		if !filepath.IsAbs(rPath) {
			rPath = filepath.Join(filepath.Dir(path), rPath)
		}

		configBytes, err := ioutil.ReadFile(rPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read relative $ref path '%v' in config '%v': %v", rPath, path, err)
		}
		configBytes = text.ReplaceEnvVariables(configBytes)

		var gen interface{}
		if err := yaml.Unmarshal(configBytes, &gen); err != nil {
			return nil, err
		}

		root = gen
		nextObj = gen
		path = rPath
	}
	if len(u.Fragment) > 0 {
		if nextObj, err = JSONPointer(u.Fragment, root); err != nil {
			return nil, fmt.Errorf("failed to resolve $ref fragment '%v' in config '%v': %v", u.Fragment, path, err)
		}
	}
	if rVal := getRefVal(nextObj); rVal != nil {
		return expandRefVal(path, level+1, root, rVal)
	}

	if _, err = refWalk(path, level+1, root, nextObj); err != nil {
		return nil, err
	}
	return nextObj, nil
}

func refWalk(path string, level int, root, obj interface{}) (refFound bool, err error) {
	switch x := obj.(type) {
	case map[string]interface{}:
		for k, v := range x {
			if rv := getRefVal(v); rv != nil {
				if x[k], err = expandRefVal(path, level, root, rv); err != nil {
					return
				}
				refFound = true
			} else {
				var rFound bool
				if rFound, err = refWalk(path, level, root, v); err != nil {
					return
				} else if rFound {
					refFound = true
				}
			}
		}
	case map[interface{}]interface{}:
		for k, v := range x {
			if rv := getRefVal(v); rv != nil {
				if x[k], err = expandRefVal(path, level, root, rv); err != nil {
					return
				}
				refFound = true
			} else {
				var rFound bool
				if rFound, err = refWalk(path, level, root, v); err != nil {
					return
				} else if rFound {
					refFound = true
				}
			}
		}
	case []interface{}:
		for i, v := range x {
			if rv := getRefVal(v); rv != nil {
				if x[i], err = expandRefVal(path, level, root, rv); err != nil {
					return
				}
				refFound = true
			} else {
				var rFound bool
				if rFound, err = refWalk(path, level, root, v); err != nil {
					return
				} else if rFound {
					refFound = true
				}
			}
		}
	}
	return
}

//------------------------------------------------------------------------------
