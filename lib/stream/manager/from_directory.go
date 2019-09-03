// Copyright (c) 2018 Ashley Jeffs
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

package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/stream"
)

//------------------------------------------------------------------------------

// LoadStreamConfigsFromDirectory reads a map of stream ids to configurations
// by walking a directory of .json and .yaml files.
func LoadStreamConfigsFromDirectory(replaceEnvVars bool, dir string) (map[string]stream.Config, error) {
	streamMap := map[string]stream.Config{}

	dir = filepath.Clean(dir)

	if info, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			return streamMap, nil
		}
		return nil, err
	} else if !info.IsDir() {
		return streamMap, nil
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() ||
			(!strings.HasSuffix(info.Name(), ".yaml") &&
				!strings.HasSuffix(info.Name(), ".json")) {
			return nil
		}

		var id string
		if id, werr = filepath.Rel(dir, path); werr != nil {
			return werr
		}
		id = strings.Trim(id, string(filepath.Separator))
		id = strings.Replace(id, string(filepath.Separator), "_", -1)

		if strings.HasSuffix(info.Name(), ".yaml") {
			id = strings.TrimSuffix(id, ".yaml")
		} else {
			id = strings.TrimSuffix(id, ".json")
		}

		if _, exists := streamMap[id]; exists {
			return fmt.Errorf("stream id (%v) collision from file: %v", id, path)
		}

		conf := config.New()
		if _, readerr := config.Read(path, true, &conf); readerr != nil {
			// TODO: Read and report linting errors.
			return readerr
		}

		streamMap[id] = conf.Config
		return nil
	})

	return streamMap, err
}

//------------------------------------------------------------------------------
