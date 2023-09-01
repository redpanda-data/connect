package cli_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	icli "github.com/benthosdev/benthos/v4/internal/cli"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func executeLintSubcmd(t *testing.T, args []string) (exitCode int, printedErr string) {
	cliApp := icli.App()
	for _, c := range cliApp.Commands {
		if c.Name == "lint" {
			c.Action = func(ctx *cli.Context) error {
				var buf bytes.Buffer
				exitCode = icli.LintAction(ctx, &buf)
				printedErr = buf.String()
				return nil
			}
		}
	}
	require.NoError(t, cliApp.Run(args))
	return
}

func TestLints(t *testing.T) {
	tmpDir := t.TempDir()
	tFile := func(name string) string {
		return filepath.Join(tmpDir, name)
	}

	tests := []struct {
		name          string
		files         map[string]string
		args          []string
		expectedCode  int
		expectedLints []string
	}{
		{
			name: "one file no errors",
			args: []string{"benthos", "lint", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    mapping: 'root.id = uuid_v4()'
output:
  drop: {}
`,
			},
		},
		{
			name: "one file unexpected fields",
			args: []string{"benthos", "lint", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    huh: what
    mapping: 'root.id = uuid_v4()'
output:
  nah: nope
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"field huh not recognised",
				"field nah is invalid",
			},
		},
		{
			name: "one file with c flag",
			args: []string{"benthos", "-c", tFile("foo.yaml"), "lint"},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    huh: what
    mapping: 'root.id = uuid_v4()'
output:
  nah: nope
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"field huh not recognised",
				"field nah is invalid",
			},
		},
		{
			name: "one file with r flag",
			args: []string{"benthos", "-r", tFile("foo.yaml"), "lint"},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    huh: what
    mapping: 'root.id = uuid_v4()'
output:
  nah: nope
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"field huh not recognised",
				"field nah is invalid",
			},
		},
		{
			name: "env var missing",
			args: []string{"benthos", "lint", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    mapping: 'root.id = "${BENTHOS_ENV_VAR_HOPEFULLY_MISSING}"'
output:
  drop: {}
`,
			},
			expectedCode: 1,
			expectedLints: []string{
				"required environment variables were not set: [BENTHOS_ENV_VAR_HOPEFULLY_MISSING]",
			},
		},
		{
			name: "env var missing but we dont care",
			args: []string{"benthos", "lint", "--skip-env-var-check", tFile("foo.yaml")},
			files: map[string]string{
				"foo.yaml": `
input:
  generate:
    mapping: 'root.id = "${BENTHOS_ENV_VAR_HOPEFULLY_MISSING}"'
output:
  drop: {}
`,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for name, c := range test.files {
				require.NoError(t, os.WriteFile(tFile(name), []byte(c), 0o644))
			}

			code, outStr := executeLintSubcmd(t, test.args)
			assert.Equal(t, test.expectedCode, code)

			if len(test.expectedLints) == 0 {
				assert.Empty(t, outStr)
			} else {
				for _, l := range test.expectedLints {
					assert.Contains(t, outStr, l)
				}
			}
		})
	}
}
