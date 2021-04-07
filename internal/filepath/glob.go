package filepath

import (
	"path/filepath"
	"runtime"
	"strings"
)

// hasMeta reports whether path contains any of the magic characters
// recognized by Match.
//
// Taken from path/filepath/match.go
func hasMeta(path string) bool {
	magicChars := `*?[`
	if runtime.GOOS != "windows" {
		magicChars = `*?[\`
	}
	return strings.ContainsAny(path, magicChars)
}

// Globs attempts to expand a list of paths, which may include glob patterns, to
// a list of explicit file paths. The paths are de-duplicated but are not
// sorted.
func Globs(paths []string) ([]string, error) {
	expandedPaths := make([]string, 0, len(paths))
	seenPaths := make(map[string]struct{}, len(paths))

	for _, path := range paths {
		globbed, err := filepath.Glob(path)
		if err != nil {
			return nil, err
		}
		for _, gPath := range globbed {
			if _, seen := seenPaths[gPath]; !seen {
				expandedPaths = append(expandedPaths, gPath)
				seenPaths[gPath] = struct{}{}
			}
		}
		if len(globbed) == 0 && !hasMeta(path) {
			if _, seen := seenPaths[path]; !seen {
				expandedPaths = append(expandedPaths, path)
				seenPaths[path] = struct{}{}
			}
		}
	}

	return expandedPaths, nil

}
