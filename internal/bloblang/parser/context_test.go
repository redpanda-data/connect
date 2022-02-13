package parser

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextImportIsolation(t *testing.T) {
	tmpDir := t.TempDir()

	content := `map foo { root = this }`
	fileName := "foo.blobl"
	fullPath := filepath.Join(tmpDir, fileName)
	require.NoError(t, os.WriteFile(fullPath, []byte(content), 0o644))

	for _, srcCtx := range []Context{GlobalContext(), EmptyContext()} {
		relCtx := srcCtx.WithImporterRelativeToFile(fullPath)
		isoCtx := srcCtx.DisabledImports()

		// Source context only works with full path
		_, err := srcCtx.importer.Import(fileName)
		assert.Error(t, err)

		out, err := srcCtx.importer.Import(fullPath)
		assert.NoError(t, err)
		assert.Equal(t, content, string(out))

		// Relative context works with full or relative path
		out, err = relCtx.importer.Import(fullPath)
		assert.NoError(t, err)
		assert.Equal(t, content, string(out))

		out, err = relCtx.importer.Import(fileName)
		assert.NoError(t, err)
		assert.Equal(t, content, string(out))

		// Isolated context doesn't work with any path
		_, err = isoCtx.importer.Import(fileName)
		assert.Error(t, err)

		_, err = isoCtx.importer.Import(fullPath)
		assert.Error(t, err)
	}
}

func TestContextImportRelativity(t *testing.T) {
	tmpDir := t.TempDir()

	for path, content := range map[string]string{
		"mappings/foo.blobl":              `map foo { root.foo = this.foo }`,
		"mappings/first/bar.blobl":        `map bar { root.bar = this.bar }`,
		"mappings/first/second/baz.blobl": `map baz { root.baz = this.baz }`,
	} {
		osPath := filepath.FromSlash(path)
		dirPath := filepath.Dir(osPath)

		require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, dirPath), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, osPath), []byte(content), 0o644))
	}

	for _, srcCtx := range []Context{GlobalContext(), EmptyContext()} {
		relCtxOne := srcCtx.WithImporterRelativeToFile(filepath.Join(tmpDir, "mappings", "foo.blobl"))
		relCtxTwo := relCtxOne.WithImporterRelativeToFile(filepath.Join("first", "bar.blobl"))

		// foo.blobl
		content, err := srcCtx.importer.Import(filepath.Join(tmpDir, "mappings", "foo.blobl"))
		require.NoError(t, err)
		assert.Equal(t, `map foo { root.foo = this.foo }`, string(content))

		content, err = relCtxOne.importer.Import("foo.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map foo { root.foo = this.foo }`, string(content))

		content, err = relCtxTwo.importer.Import("../foo.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map foo { root.foo = this.foo }`, string(content))

		// bar.blobl
		content, err = srcCtx.importer.Import(filepath.Join(tmpDir, "mappings", "first", "bar.blobl"))
		require.NoError(t, err)
		assert.Equal(t, `map bar { root.bar = this.bar }`, string(content))

		content, err = relCtxOne.importer.Import("./first/bar.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map bar { root.bar = this.bar }`, string(content))

		content, err = relCtxTwo.importer.Import("bar.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map bar { root.bar = this.bar }`, string(content))

		// baz.blobl
		content, err = srcCtx.importer.Import(filepath.Join(tmpDir, "mappings", "first", "second", "baz.blobl"))
		require.NoError(t, err)
		assert.Equal(t, `map baz { root.baz = this.baz }`, string(content))

		content, err = relCtxOne.importer.Import("./first/second/baz.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map baz { root.baz = this.baz }`, string(content))

		content, err = relCtxTwo.importer.Import("second/baz.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map baz { root.baz = this.baz }`, string(content))
	}
}

func TestContextCustomImports(t *testing.T) {
	mappings := map[string]string{
		"mappings/foo.blobl":              `map foo { root.foo = this.foo }`,
		"mappings/first/bar.blobl":        `map bar { root.bar = this.bar }`,
		"mappings/first/second/baz.blobl": `map baz { root.baz = this.baz }`,
	}

	importer := func(name string) ([]byte, error) {
		name = path.Clean(name)
		s, ok := mappings[name]
		if !ok {
			return nil, fmt.Errorf("mapping %v not found", name)
		}
		return []byte(s), nil
	}

	for _, srcCtx := range []Context{GlobalContext().CustomImporter(importer), EmptyContext().CustomImporter(importer)} {
		relCtxOne := srcCtx.WithImporterRelativeToFile(path.Join("mappings", "foo.blobl"))
		relCtxTwo := relCtxOne.WithImporterRelativeToFile(path.Join("first", "bar.blobl"))

		// foo.blobl
		content, err := srcCtx.importer.Import(path.Join("mappings", "foo.blobl"))
		require.NoError(t, err)
		assert.Equal(t, `map foo { root.foo = this.foo }`, string(content))

		content, err = relCtxOne.importer.Import("foo.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map foo { root.foo = this.foo }`, string(content))

		content, err = relCtxTwo.importer.Import("../foo.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map foo { root.foo = this.foo }`, string(content))

		// bar.blobl
		content, err = srcCtx.importer.Import(path.Join("mappings", "first", "bar.blobl"))
		require.NoError(t, err)
		assert.Equal(t, `map bar { root.bar = this.bar }`, string(content))

		content, err = relCtxOne.importer.Import("./first/bar.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map bar { root.bar = this.bar }`, string(content))

		content, err = relCtxTwo.importer.Import("bar.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map bar { root.bar = this.bar }`, string(content))

		// baz.blobl
		content, err = srcCtx.importer.Import(path.Join("mappings", "first", "second", "baz.blobl"))
		require.NoError(t, err)
		assert.Equal(t, `map baz { root.baz = this.baz }`, string(content))

		content, err = relCtxOne.importer.Import("./first/second/baz.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map baz { root.baz = this.baz }`, string(content))

		content, err = relCtxTwo.importer.Import("second/baz.blobl")
		require.NoError(t, err)
		assert.Equal(t, `map baz { root.baz = this.baz }`, string(content))
	}
}
