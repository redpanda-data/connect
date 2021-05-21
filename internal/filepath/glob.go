package filepath

import (
	"os"
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
	var expandedPaths []string
	seenPaths := map[string]struct{}{}

	for _, path := range paths {
		var globbed []string
		var err error
		if segments := strings.Split(path, "**"); len(segments) == 1 {
			globbed, err = filepath.Glob(path)
		} else {
			globbed, err = superGlobs(segments)
		}
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

// Inspired by https://github.com/yargevad/filepathx/blob/master/filepathx.go
func superGlobs(segments []string) ([]string, error) {
	matches := map[string]struct{}{"": {}}

	for i, segment := range segments {
		newMatches := map[string]struct{}{}
		lastSegment := (len(segments) - 1) == i

		for match := range matches {
			paths, err := filepath.Glob(match + segment)
			if err != nil {
				return nil, err
			}
			for _, path := range paths {
				if err := filepath.Walk(path, func(newPath string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if lastSegment && info.IsDir() {
						return nil
					}
					newMatches[newPath] = struct{}{}
					return nil
				}); err != nil {
					return nil, err
				}
			}
		}

		matches = newMatches
	}

	matchSlice := make([]string, 0, len(matches))
	for path := range matches {
		matchSlice = append(matchSlice, path)
	}
	return matchSlice, nil
}
