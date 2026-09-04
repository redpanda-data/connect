// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package avro

import (
	"bytes"
	"compress/flate"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestScanner(t *testing.T) {
	tests := []struct {
		name    string
		rawJSON bool
		output  []string
	}{
		{
			name:    "standard JSON",
			rawJSON: false,
			output: []string{
				`{"Price":{"double":12.32},"OrderDate":{"long.timestamp-millis":1687221496000},"OrderStatus":{"string":"Canceled"},"Email":{"string":"elizabeth.brown@example.com"},"Quantity":{"long":5}}`,
				`{"Email":{"string":"james.wilson@example.com"},"Quantity":{"long":5},"Price":{"double":12.35},"OrderDate":{"long.timestamp-millis":1702926589000},"OrderStatus":{"string":"Pending"}}`,
				`{"OrderDate":{"long.timestamp-millis":1708606337000},"OrderStatus":{"string":"Completed"},"Email":{"string":"kristin.walls@example.com"},"Quantity":{"long":6},"Price":{"double":10.3}}`,
			},
		},
		{
			name:    "AVRO JSON",
			rawJSON: true,
			output: []string{
				`{"Email":"elizabeth.brown@example.com","OrderDate":1.687221496e+12,"OrderStatus":"Canceled","Price":12.32,"Quantity":5}`,
				`{"Email":"james.wilson@example.com","OrderDate":1.702926589e+12,"OrderStatus":"Pending","Price":12.35,"Quantity":5}`,
				`{"Email":"kristin.walls@example.com","OrderDate":1.708606337e+12,"OrderStatus":"Completed","Price":10.3,"Quantity":6}`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
			pConf, err := confSpec.ParseYAML(fmt.Sprintf(`
test:
  avro:
    raw_json: %t
`, test.rawJSON), nil)
			require.NoError(t, err)

			rdr, err := pConf.FieldScanner("test")
			require.NoError(t, err)

			b, err := os.ReadFile("./resources/ocf.avro")
			require.NoError(t, err)

			buf := bytes.NewReader(b)
			var acked bool
			strm, err := rdr.Create(io.NopCloser(buf), func(context.Context, error) error {
				acked = true
				return nil
			}, service.NewScannerSourceDetails())
			require.NoError(t, err)

			for _, s := range test.output {
				m, aFn, err := strm.NextBatch(t.Context())
				require.NoError(t, err)
				require.Len(t, m, 1)
				mBytes, err := m[0].AsBytes()
				require.NoError(t, err)
				assert.JSONEq(t, s, string(mBytes))
				require.NoError(t, aFn(t.Context(), nil))
				assert.False(t, acked)
			}

			_, _, err = strm.NextBatch(t.Context())
			require.Equal(t, io.EOF, err)

			require.NoError(t, strm.Close(t.Context()))
			assert.True(t, acked)
		})
	}
}

// buildOCF writes a single `bytes`-schema datum of datumLen bytes into an OCF
// stream using the given upstream codec. Fixtures built with the stock writer
// codecs ensure the scanner is exercised against standard OCF framing a real
// producer would emit.
func buildOCF(t *testing.T, codec ocf.Codec, datumLen int) []byte {
	t.Helper()
	schema := avro.MustParse(`"bytes"`)
	var buf bytes.Buffer
	w, err := ocf.NewWriter(&buf, schema, ocf.WithCodec(codec))
	require.NoError(t, err)
	datum := make([]byte, datumLen)
	require.NoError(t, w.Encode(&datum))
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// scanOCF runs an OCF byte stream through the `avro` scanner configured with
// the given YAML and returns the error from the first NextBatch.
func scanOCF(t *testing.T, confYAML string, ocfBytes []byte) error {
	t.Helper()
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(confYAML, nil)
	require.NoError(t, err)
	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	strm, err := rdr.Create(
		io.NopCloser(bytes.NewReader(ocfBytes)),
		func(context.Context, error) error { return nil },
		service.NewScannerSourceDetails(),
	)
	require.NoError(t, err)
	defer strm.Close(t.Context())
	_, _, err = strm.NextBatch(t.Context())
	return err
}

// capConf returns scanner config YAML setting an explicit decompressed-block cap.
func capConf(n int) string {
	return fmt.Sprintf("test:\n  avro:\n    max_decompressed_block_bytes: %d\n", n)
}

// TestScannerDecompressedSizeGuard is the regression test for the Avro OCF
// deflate-bomb (decompression-amplification DoS). With the scanner's
// decompressed-block cap (default 16 MiB, or a configured value) a small block
// that expands past the cap is rejected before the expanded datum — and its
// even-larger JSON encoding — is materialized, while legitimate blocks under
// the cap still decode. Covered for every built-in compressed codec.
//
// The assertions are deliberately behavioural rather than message-matching: the
// upstream wording differs per codec (deflate and snappy report "exceeds limit
// of N bytes", while zstandard surfaces klauspost/compress's own error, itself
// varying with which bound trips), and upstream exports no error sentinel to
// match on. Rejection is instead pinned down by showing the *same fixture*
// decodes once the cap is raised above it — which proves the cap is what
// rejected the block, not a malformed fixture.
func TestScannerDecompressedSizeGuard(t *testing.T) {
	const defaultConf = "test:\n  avro: {}\n"

	codecs := []struct {
		name  string
		codec ocf.Codec
	}{
		{"deflate", ocf.DeflateCodec(flate.BestCompression)},
		{"snappy", ocf.SnappyCodec()},
		{"zstandard", ocf.MustZstdCodec(nil, nil)},
	}

	for _, c := range codecs {
		t.Run(c.name, func(t *testing.T) {
			t.Run("legit block under default cap decodes", func(t *testing.T) {
				// 8 MiB < 16 MiB default — must not be rejected.
				require.NoError(t, scanOCF(t, defaultConf, buildOCF(t, c.codec, 8<<20)))
			})

			t.Run("bomb over default cap rejected", func(t *testing.T) {
				// 24 MiB (the researcher's PoC size) > 16 MiB default.
				ocfBytes := buildOCF(t, c.codec, 24<<20)
				require.Less(t, len(ocfBytes), 4<<20,
					"fixture should stay small on the wire (%d bytes) — that's the amplification", len(ocfBytes))
				require.Error(t, scanOCF(t, defaultConf, ocfBytes),
					"scanner accepted a decompression bomb")
			})

			t.Run("cap is what rejects, not the fixture", func(t *testing.T) {
				// One fixture, two caps: rejected under a cap below its
				// decompressed size, decoded under one above it. Kept small so
				// the accepted case doesn't materialize a large JSON message.
				ocfBytes := buildOCF(t, c.codec, 2<<20)
				require.Error(t, scanOCF(t, capConf(1<<20), ocfBytes))
				require.NoError(t, scanOCF(t, capConf(4<<20), ocfBytes))
			})

			t.Run("configured cap is honored", func(t *testing.T) {
				// A 4 MiB explicit cap rejects an 8 MiB block that the default
				// would accept — proving the config field is wired through.
				require.Error(t, scanOCF(t, capConf(4<<20), buildOCF(t, c.codec, 8<<20)))
				// ...and a block under that explicit cap still decodes.
				require.NoError(t, scanOCF(t, capConf(4<<20), buildOCF(t, c.codec, 1<<20)))
			})
		})
	}
}

// TestScannerMaxDecompressedBlockBytesLint pins the config-lint boundary for
// max_decompressed_block_bytes. A negative value is meaningless as a byte
// count and, left unvalidated, is silently coerced by the underlying library
// to its own 64 MiB default (see ocf.WithMaxDecompressedBlockBytes) rather
// than surfaced as a config error — exactly the kind of typo (e.g. a stray
// -1 copied from another connector's "unlimited" convention) that
// CONTRIBUTING.md's config-validation guidance calls out. Zero is a
// documented, valid sentinel ("use the underlying library default") and must
// not be flagged.
func TestScannerMaxDecompressedBlockBytesLint(t *testing.T) {
	linter := service.GlobalEnvironment().NewComponentConfigLinter()

	lints, err := linter.LintScannerYAML(fmt.Appendf(nil, "avro:\n  %s: -1\n", sFieldMaxDecompressedBlockBytes))
	require.NoError(t, err)
	assert.NotEmptyf(t, lints, "%s: -1 should be rejected by config lint", sFieldMaxDecompressedBlockBytes)

	lints, err = linter.LintScannerYAML(fmt.Appendf(nil, "avro:\n  %s: 0\n", sFieldMaxDecompressedBlockBytes))
	require.NoError(t, err)
	assert.Emptyf(t, lints, "%s: 0 is the documented library-default sentinel and must not lint", sFieldMaxDecompressedBlockBytes)
}
