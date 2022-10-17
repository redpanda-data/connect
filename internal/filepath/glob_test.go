package filepath

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

func TestGlobPatterns(t *testing.T) {
	dirStructure := []string{
		`src/cats/a.js`,
		`src/cats/b.js`,
		`src/cats/b.txt`,
		`src/cats/toys`,
		`src/cats/meows/c.js`,
		`src/cats/meows/c.js.tmp`,
	}

	tmpDir := t.TempDir()

	for _, path := range dirStructure {
		tmpPath := filepath.Join(tmpDir, path)
		if filepath.Ext(tmpPath) == "" {
			require.NoError(t, os.MkdirAll(tmpPath, 0o755))
		} else {
			require.NoError(t, os.MkdirAll(filepath.Dir(tmpPath), 0o755))
			require.NoError(t, os.WriteFile(tmpPath, []byte("keep me"), 0o755))
		}
	}

	tests := []struct {
		pattern string
		matches []string
	}{
		{
			pattern: `/src/cats/*.js`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
			},
		},
		{
			pattern: `/src/cats/a.js`,
			matches: []string{
				`src/cats/a.js`,
			},
		},
		{
			pattern: `/src/cats/z.js`,
			matches: []string{
				`src/cats/z.js`,
			},
		},
		{
			pattern: `/src/**/a.js`,
			matches: []string{
				`src/cats/a.js`,
			},
		},
		{
			pattern: `/src/**/*.js`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
				`src/cats/meows/c.js`,
			},
		},
		{
			pattern: `/src/**/*`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
				`src/cats/b.txt`,
				`src/cats/meows/c.js`,
				`src/cats/meows/c.js.tmp`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.pattern, func(t *testing.T) {
			matches, err := Globs(ifs.OS(), []string{tmpDir + test.pattern})
			require.NoError(t, err)

			for i, match := range matches {
				matches[i], err = filepath.Rel(tmpDir, match)
				require.NoError(t, err)
			}
			assert.ElementsMatch(t, test.matches, matches)
		})
	}
}

func TestGlobsAndSuperPaths(t *testing.T) {
	dirStructure := []string{
		`src/cats/a.js`,
		`src/cats/b.js`,
		`src/cats/b.txt`,
		`src/cats/toys`,
		`src/cats/meows/c.js`,
		`src/cats/meows/c.js.tmp`,
	}

	tmpDir := t.TempDir()

	for _, path := range dirStructure {
		tmpPath := filepath.Join(tmpDir, path)
		if filepath.Ext(tmpPath) == "" {
			require.NoError(t, os.MkdirAll(tmpPath, 0o755))
		} else {
			require.NoError(t, os.MkdirAll(filepath.Dir(tmpPath), 0o755))
			require.NoError(t, os.WriteFile(tmpPath, []byte("keep me"), 0o755))
		}
	}

	tests := []struct {
		pattern string
		matches []string
	}{
		{
			pattern: `/src/cats/*.js`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
			},
		},
		{
			pattern: `/src/cats/a.js`,
			matches: []string{
				`src/cats/a.js`,
			},
		},
		{
			pattern: `/src/cats/z.js`,
			matches: []string{
				`src/cats/z.js`,
			},
		},
		{
			pattern: `/src/**/a.js`,
			matches: []string{
				`src/cats/a.js`,
			},
		},
		{
			pattern: `/src/**/*.js`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
				`src/cats/meows/c.js`,
			},
		},
		{
			pattern: `/src/**/*`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
				`src/cats/b.txt`,
				`src/cats/meows/c.js`,
				`src/cats/meows/c.js.tmp`,
			},
		},
		{
			pattern: `/src/...`,
			matches: []string{
				`src/cats/a.js`,
				`src/cats/b.js`,
				`src/cats/meows/c.js`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.pattern, func(t *testing.T) {
			matches, err := GlobsAndSuperPaths(ifs.OS(), []string{tmpDir + test.pattern}, "js")
			require.NoError(t, err)

			for i, match := range matches {
				matches[i], err = filepath.Rel(tmpDir, match)
				require.NoError(t, err)
			}
			assert.ElementsMatch(t, test.matches, matches)
		})
	}
}
