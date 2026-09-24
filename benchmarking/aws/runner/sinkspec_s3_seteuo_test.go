// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// sidecarFrame is one parsed "###timestamp=..." block from the artifact
// file a running s3SidecarSetup loop appends to.
type sidecarFrame struct {
	sizePresent bool
	size        string
	records     string
}

// parseSidecarFrames splits the artifact file's raw content into per-poll
// frames, the same shape ParseIcebergSeries consumes downstream.
func parseSidecarFrames(t *testing.T, raw string) []sidecarFrame {
	t.Helper()
	var (
		frames []sidecarFrame
		cur    *sidecarFrame
	)
	for _, line := range strings.Split(raw, "\n") {
		switch {
		case strings.HasPrefix(line, "###timestamp="):
			if cur != nil {
				frames = append(frames, *cur)
			}
			cur = &sidecarFrame{}
		case strings.HasPrefix(line, "total_files_size_bytes "):
			if cur == nil {
				t.Fatalf("total_files_size_bytes line before any ###timestamp frame:\n%s", raw)
			}
			cur.sizePresent = true
			cur.size = strings.TrimPrefix(line, "total_files_size_bytes ")
		case strings.HasPrefix(line, "total_records "):
			if cur == nil {
				t.Fatalf("total_records line before any ###timestamp frame:\n%s", raw)
			}
			cur.records = strings.TrimPrefix(line, "total_records ")
		}
	}
	if cur != nil {
		frames = append(frames, *cur)
	}
	return frames
}

// writeMockBinary drops a POSIX shell script onto dir/name and makes it
// executable, so it can be placed ahead of the real PATH.
func writeMockBinary(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body), 0o755); err != nil {
		t.Fatalf("write mock %s: %v", name, err)
	}
	return p
}

// sidecarHarness is one scenario's mock behavior for the sidecar's two
// external dependencies.
type sidecarHarness struct {
	engine     string
	awsMode    string // "fail" | "empty" | "multipage" | "" (single-page numeric)
	kcListMode string // "match" | "nomatch" | "" (kafka_connect --list output)
	iterations int
}

// runSidecarUnderSetE renders s3SidecarSetup's real output and executes it
// verbatim under `sh -euo pipefail` -- the exact shell mode
// renderBenchScript's `set -euo pipefail` puts it in (matrix.go), which the
// sidecar's own `( while ... ) &` background subshell inherits. `aws` and a
// mocked `sleep` are placed on PATH; the sidecar's own
// `/opt/kafka/bin/kafka-consumer-groups.sh` calls use a hardcoded absolute
// path (not a PATH lookup), so PATH alone can't intercept them -- the
// literal absolute-path string is substituted for a mock binary's real path
// instead, since this machine has no write access to /opt. Every other line
// of the generated script (the awk pipelines, the AWSFAIL sentinel logic,
// the quoting) runs completely unmodified.
//
// The mocked `sleep` is what makes the loop terminate deterministically
// instead of by timing: each invocation increments a counter file, and once
// the counter reaches `iterations` it kills $PID (exported into the
// script's environment) and blocks until it's actually dead before
// returning, so the outer `while kill -0 "$PID"` loop is guaranteed to see
// the death on its very next check -- exactly `iterations` frames get
// written, no more, no fewer, regardless of real wall-clock scheduling.
//
// The mocked `aws` binary's "empty" mode inspects the actual --query
// argument the sidecar's real code passed it, rather than always behaving
// the same way, so this harness can tell the two query strings apart
// exactly the way the real AWS CLI does: `sum(Contents[].Size)` (the old,
// buggy query) is a genuine jmespath type error against an empty prefix's
// absent Contents key, so the CLI itself exits non-zero; `Contents[].Size`
// (the fixed query) just projects to nothing against that same absent key,
// so the CLI exits 0 with no output at all. That is what makes
// TestS3SidecarSetup_KafkaConnectColdStartEmptyPrefixYieldsUsableFrames a
// real regression test rather than a tautology: it fails against the old
// query and passes against the new one, driven by the query string
// s3SidecarSetup actually emits, not by a hardcoded switch in the test.
func runSidecarUnderSetE(t *testing.T, h sidecarHarness) (frames []sidecarFrame, stderr, raw string) {
	t.Helper()
	for _, bin := range []string{"sh", "awk", "printf"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not available on PATH", bin)
		}
	}

	mockDir := t.TempDir()
	writeMockBinary(t, mockDir, "aws", `
case "$1 $2" in
"s3api list-objects-v2")
  QUERY=""
  prev=""
  for a in "$@"; do
    if [ "$prev" = "--query" ]; then QUERY="$a"; fi
    prev="$a"
  done
  case "$MOCK_AWS_MODE" in
    fail) exit 1 ;;
    empty)
      case "$QUERY" in
        *'sum('*) exit 1 ;;
        *) exit 0 ;;
      esac
      ;;
    multipage) printf '12345 6789\n111\n' ;;
    *) echo "0" ;;
  esac
  ;;
*) exit 1 ;;
esac
`)
	writeMockBinary(t, mockDir, "sleep", `
N=$(cat "$MOCK_SLEEP_COUNT_FILE" 2>/dev/null || echo 0)
N=$((N + 1))
echo "$N" > "$MOCK_SLEEP_COUNT_FILE"
if [ "$N" -ge "$MOCK_SLEEP_MAX_ITERS" ]; then
  kill "$PID" 2>/dev/null || true
  while kill -0 "$PID" 2>/dev/null; do
    :
  done
fi
exit 0
`)
	kcMock := writeMockBinary(t, mockDir, "mock-kafka-consumer-groups.sh", `
CMD=""
GRP=""
prev=""
for a in "$@"; do
  case "$a" in
    --list) CMD=list ;;
    --describe) CMD=describe ;;
  esac
  if [ "$prev" = "--group" ]; then GRP="$a"; fi
  prev="$a"
done
case "$CMD" in
  list)
    case "$MOCK_KC_LIST_MODE" in
      match) echo "connect-bench_s3_v4-1a2b3c" ;;
      nomatch) echo "some-other-group" ;;
      *) : ;;
    esac
    exit 0
    ;;
  describe)
    echo "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG"
    echo "$GRP topic1 0 7 7 0"
    exit 0
    ;;
  *) exit 1 ;;
esac
`)

	args := MetricSidecarArgs{
		Engine: h.engine,
		VCPU:   4,
		Outs:   s3Outs(),
		Names:  newBenchNames("sess", "s3"),
	}
	artifact := fmt.Sprintf("setoe-%s.txt", strings.NewReplacer("/", "_", " ", "_").Replace(t.Name()))
	setup := s3SidecarSetup(args, artifact)
	setup = strings.ReplaceAll(setup, "/opt/kafka/bin/kafka-consumer-groups.sh", kcMock)

	// s3SidecarSetup hardcodes "RP=/tmp/<artifact>" itself (matching every
	// other sidecar/log path this codebase writes -- see matrix.go's
	// /tmp/bench-N.log), so the artifact must be read back from literal
	// /tmp, not t.TempDir().
	rp := filepath.Join("/tmp", artifact)
	t.Cleanup(func() { _ = os.Remove(rp) })

	countFile := filepath.Join(mockDir, "sleep-count")

	script := fmt.Sprintf(`set -euo pipefail
: > %q
/bin/sleep 300 &
PID=$!
export PID
trap 'kill "$PID" 2>/dev/null || true' EXIT
%s
wait "$RP_SCRAPER" 2>/dev/null || true
`, countFile, setup)

	cmd := exec.Command("sh", "-c", script)
	cmd.Env = append(os.Environ(),
		"PATH="+mockDir+":"+os.Getenv("PATH"),
		"MOCK_AWS_MODE="+h.awsMode,
		"MOCK_KC_LIST_MODE="+h.kcListMode,
		"MOCK_SLEEP_COUNT_FILE="+countFile,
		fmt.Sprintf("MOCK_SLEEP_MAX_ITERS=%d", h.iterations),
	)
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf
	if err := cmd.Run(); err != nil {
		t.Fatalf("sidecar script exited non-zero under `sh -euo pipefail` -- this is exactly the live failure mode (the loop died silently after one frame): %v\nstderr:\n%s",
			err, stderrBuf.String())
	}

	rawBytes, err := os.ReadFile(rp)
	if err != nil {
		t.Fatalf("reading sidecar artifact %s: %v", rp, err)
	}
	raw = string(rawBytes)
	frames = parseSidecarFrames(t, raw)
	if len(frames) < h.iterations {
		t.Fatalf("expected at least %d frames (one per mocked sleep call before $PID was killed), got %d:\n%s",
			h.iterations, len(frames), raw)
	}
	return frames, stderrBuf.String(), raw
}

// TestS3SidecarSetup_SurvivesAWSFailureUnderSetE covers a genuine `aws
// s3api list-objects-v2` failure (bad credentials, missing bucket, a real
// transient AWS API error) -- NOT an empty prefix, which is now a
// legitimate zero handled by
// TestS3SidecarSetup_KafkaConnectColdStartEmptyPrefixYieldsUsableFrames
// below. This is still exactly the shape of the live-infra regression
// this whole set -e handling exists for: before the AWSFAIL-sentinel fix,
// a bare `S_RAW=$(aws ...)` assignment whose command substitution failed
// was fatal under `set -e` on that exact line -- the very next line's `if
// [ $? -ne 0 ]` was never reached, and the whole background subshell died,
// producing exactly one frame and then nothing for the rest of the run
// (observed live, though the trigger for that specific incident was the
// old query's empty-prefix type error, not a genuine failure -- see this
// file's other tests for that distinction). A passing run here proves the
// loop survives ANY genuine aws-cli failure, keeps polling, and correctly
// omits (not zeroes) total_files_size_bytes while still emitting
// total_records every frame.
func TestS3SidecarSetup_SurvivesAWSFailureUnderSetE(t *testing.T) {
	const iterations = 3
	frames, _, _ := runSidecarUnderSetE(t, sidecarHarness{
		engine:     "connect",
		awsMode:    "fail",
		iterations: iterations,
	})
	if len(frames) < iterations {
		t.Fatalf("expected >= %d frames, got %d", iterations, len(frames))
	}
	for i, f := range frames {
		if f.sizePresent {
			t.Errorf("frame %d: total_files_size_bytes must be omitted (not zeroed) when aws fails, got %q", i, f.size)
		}
		if f.records == "" {
			t.Errorf("frame %d: total_records must still be emitted when aws fails", i)
		}
	}
}

// TestS3SidecarSetup_EmptyPrefixIsTrueZero covers the legitimate-zero case
// this whole fix is about: an empty prefix (the "aws" mock prints nothing
// and exits 0, exactly what `--query "Contents[].Size"` against a
// Contents-less ListObjectsV2 response does for real) must produce a real
// total_files_size_bytes 0 line, distinct from the omitted-on-failure case
// above.
func TestS3SidecarSetup_EmptyPrefixIsTrueZero(t *testing.T) {
	const iterations = 2
	frames, _, _ := runSidecarUnderSetE(t, sidecarHarness{
		engine:     "connect",
		awsMode:    "empty",
		iterations: iterations,
	})
	for i, f := range frames {
		if !f.sizePresent {
			t.Fatalf("frame %d: total_files_size_bytes must be present when aws succeeds:\n%+v", i, f)
		}
		if f.size != "0" {
			t.Errorf("frame %d: total_files_size_bytes = %q, want 0 for a genuinely empty prefix", i, f.size)
		}
	}
}

// TestS3SidecarSetup_KafkaConnectColdStartEmptyPrefixYieldsUsableFrames
// locks in the exact live regression this fix targets: at the start of a
// Kafka Connect sweep point, the engine's S3 prefix is empty for the KC
// cold-start window (observed live as ~10 minutes with 0 objects written),
// while total_records is already a real value (0 or advancing, never a
// failure). Before this fix, `--query "sum(Contents[].Size)"` against that
// empty prefix was a guaranteed jmespath type error, which the AWSFAIL
// sentinel correctly detected and reacted to by OMITTING
// total_files_size_bytes for the frame -- but ParseIcebergSeries gates an
// entire frame-PAIR's output on total_files_size_bytes being present on
// BOTH frames, so every one of those KC-cold-start frames was dropped
// downstream, records included. That is precisely how a real sweep point
// captured 0 metric samples and the runner aborted. This test proves both
// halves of the fix: the mocked "aws" binary reacts to the real --query
// string s3SidecarSetup emits (see runSidecarUnderSetE's doc comment), so
// it fails here against the old "sum(Contents[].Size)" query and passes
// against the fixed "Contents[].Size" query; and the resulting dump, fed
// through the exact same ParseIcebergSeries the runner uses downstream,
// yields a usable point for every consecutive frame pair instead of
// dropping them all.
func TestS3SidecarSetup_KafkaConnectColdStartEmptyPrefixYieldsUsableFrames(t *testing.T) {
	const iterations = 4
	frames, _, raw := runSidecarUnderSetE(t, sidecarHarness{
		engine:     "kafka_connect",
		awsMode:    "empty",
		kcListMode: "match",
		iterations: iterations,
	})
	for i, f := range frames {
		if !f.sizePresent {
			t.Fatalf("frame %d: total_files_size_bytes must be present (a real zero) during a KC cold-start empty prefix, not omitted -- this is the exact live failure mode:\n%s", i, raw)
		}
		if f.size != "0" {
			t.Errorf("frame %d: total_files_size_bytes = %q, want 0 for an empty prefix", i, f.size)
		}
		if f.records == "" {
			t.Errorf("frame %d: total_records must still be emitted", i)
		}
	}

	// ParseIcebergSeries is exercised against a synthetic dump built from
	// these same parsed frames rather than the raw artifact verbatim: the
	// mocked `sleep` above returns near-instantly (that's what makes this
	// test deterministic instead of timing-dependent -- see
	// runSidecarUnderSetE's doc comment), so every real frame lands in the
	// same wall-clock second and ParseIcebergSeries's own
	// "interval <= 0" out-of-order guard would drop every frame pair
	// regardless of whether total_files_size_bytes is present. Assigning
	// fabricated, strictly increasing 10s-apart timestamps here isolates
	// the exact thing this test is about -- whether hasB gates the frame
	// pair -- from that unrelated same-second collision.
	const pollIntervalSecs = 10
	var sb strings.Builder
	for i, f := range frames {
		fmt.Fprintf(&sb, "###timestamp=%d\n", 1700000000+i*pollIntervalSecs)
		if f.sizePresent {
			fmt.Fprintf(&sb, "total_files_size_bytes %s\n", f.size)
		}
		fmt.Fprintf(&sb, "total_records %s\n", f.records)
	}
	points, err := ParseIcebergSeries(strings.NewReader(sb.String()))
	if err != nil {
		t.Fatalf("ParseIcebergSeries: %v", err)
	}
	if want := iterations - 1; len(points) != want {
		t.Fatalf("ParseIcebergSeries dropped frames it should have kept: got %d usable points, want %d -- this is exactly the live failure (a sweep point capturing 0 metric samples) if it regresses:\n%s",
			len(points), want, sb.String())
	}
}

// TestS3SidecarSetup_AWSMultiPagePagination locks in the pagination fix:
// `aws s3api list-objects-v2 --query "Contents[].Size" --output text`
// applies --query PER PAGE, so a prefix crossing 1000 objects prints one
// page's sizes per line ("12345 6789\n111" here) rather than one grand
// total. The awk program must sum every field on every line (19245), not
// reject the multi-token output.
func TestS3SidecarSetup_AWSMultiPagePagination(t *testing.T) {
	const iterations = 2
	frames, _, _ := runSidecarUnderSetE(t, sidecarHarness{
		engine:     "connect",
		awsMode:    "multipage",
		iterations: iterations,
	})
	for i, f := range frames {
		if !f.sizePresent {
			t.Fatalf("frame %d: total_files_size_bytes must be present on a successful multi-page call:\n%+v", i, f)
		}
		if want := "19245"; f.size != want {
			t.Errorf("frame %d: total_files_size_bytes = %q, want %q (12345+6789+111 summed across pages)", i, f.size, want)
		}
	}
}

// TestS3SidecarSetup_KafkaConnectGroupNotFoundYet covers the
// kafka_connect-engine discovery path at the start of a sweep point, before
// the connector's consumer group has been created: `kafka-consumer-groups.sh
// --list | grep -F <connector>` finds nothing, which is exactly the
// "pipeline whose last command found no match" shape that's fatal under
// `pipefail` unless every stage tolerates it. The loop must survive, keep
// emitting frames (total_records 0, since no group means nothing to sum),
// and fire the loud diagnostic warning on stderr rather than silently
// reporting 0 forever undetected.
func TestS3SidecarSetup_KafkaConnectGroupNotFoundYet(t *testing.T) {
	const iterations = 3
	frames, stderr, _ := runSidecarUnderSetE(t, sidecarHarness{
		engine:     "kafka_connect",
		awsMode:    "empty",
		kcListMode: "nomatch",
		iterations: iterations,
	})
	if len(frames) < iterations {
		t.Fatalf("expected >= %d frames despite no matching KC consumer group, got %d", iterations, len(frames))
	}
	for i, f := range frames {
		if f.records != "0" {
			t.Errorf("frame %d: total_records = %q, want 0 when no consumer group is discovered", i, f.records)
		}
	}
	if !strings.Contains(stderr, "no kafka_connect consumer group found matching connector") {
		t.Errorf("expected the loud no-group-found warning on stderr, got:\n%s", stderr)
	}
}

// TestS3SidecarSetup_KafkaConnectGroupFound is the positive counterpart:
// once the connector's group exists, --list | grep -F finds it and the
// describe loop sums a real CURRENT-OFFSET.
func TestS3SidecarSetup_KafkaConnectGroupFound(t *testing.T) {
	const iterations = 2
	frames, stderr, _ := runSidecarUnderSetE(t, sidecarHarness{
		engine:     "kafka_connect",
		awsMode:    "empty",
		kcListMode: "match",
		iterations: iterations,
	})
	for i, f := range frames {
		if f.records != "7" {
			t.Errorf("frame %d: total_records = %q, want 7 (mocked CURRENT-OFFSET) once the group is discovered", i, f.records)
		}
	}
	if strings.Contains(stderr, "no kafka_connect consumer group found matching connector") {
		t.Errorf("must not warn once a matching group is discovered:\n%s", stderr)
	}
}
