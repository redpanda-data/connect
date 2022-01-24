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
//
// Deprecated: The streams builder is using ./internal/config now.
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
		id = strings.ReplaceAll(id, string(filepath.Separator), "_")

		if strings.HasSuffix(info.Name(), ".yaml") {
			id = strings.TrimSuffix(id, ".yaml")
		} else {
			id = strings.TrimSuffix(id, ".json")
		}

		if _, exists := streamMap[id]; exists {
			return fmt.Errorf("stream id (%v) collision from file: %v", id, path)
		}

		conf := config.New()
		if _, readerr := config.ReadV2(path, true, false, &conf); readerr != nil {
			// TODO: Read and report linting errors.
			return readerr
		}

		streamMap[id] = conf.Config
		return nil
	})

	return streamMap, err
}

//------------------------------------------------------------------------------
