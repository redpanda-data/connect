package main_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/template"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestTemplateTesting(t *testing.T) {
	testTemplatesDir := "../../../template/test"
	files, err := os.ReadDir(testTemplatesDir)
	require.NoError(t, err)

	for _, f := range files {
		t.Run(f.Name(), func(t *testing.T) {
			conf, lints, err := template.ReadConfig(filepath.Join(testTemplatesDir, f.Name()))
			require.NoError(t, err)
			assert.Empty(t, lints)

			testErrs, err := conf.Test()
			require.NoError(t, err)
			assert.Empty(t, testErrs)
		})
	}
}
