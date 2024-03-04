package studio_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	icli "github.com/benthosdev/benthos/v4/internal/cli"
	"github.com/benthosdev/benthos/v4/internal/cli/studio"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type validateRequestFn func(t *testing.T, w http.ResponseWriter, r *http.Request)

type tExpectedRequest struct {
	path string
	fn   validateRequestFn
}

func expectedRequest(path string, fn validateRequestFn) tExpectedRequest {
	return tExpectedRequest{path: path, fn: fn}
}

func testServerForPullRunner(
	t *testing.T,
	nowFn func() time.Time,
	args []string,
	expectedRequests ...tExpectedRequest,
) (pr *studio.PullRunner, waitFn func(context.Context)) {
	if nowFn == nil {
		nowFn = time.Now
	}

	doneChan := make(chan struct{})
	var closeOnce sync.Once
	waitFn = func(ctx context.Context) {
		select {
		case <-ctx.Done():
			t.Error("requests never finished")
		case <-doneChan:
		}
	}
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, len(expectedRequests))

		expReq := expectedRequests[0]
		expectedRequests = expectedRequests[1:]
		if len(expectedRequests) == 0 {
			defer func() {
				closeOnce.Do(func() {
					close(doneChan)
				})
			}()
		}

		t.Logf("request: %v", expReq.path)

		require.Equal(t, expReq.path, r.URL.EscapedPath())

		// Verify that our authorization tokens are punching through
		assert.Equal(t, "aaa", r.Header.Get("X-Bstdio-Node-Id"))
		assert.Equal(t, "Node bbb", r.Header.Get("Authorization"))

		expReq.fn(t, w, r)
	}))

	injectedArgs := make([]string, 0, len(args)+2)
	for _, a := range args {
		injectedArgs = append(injectedArgs, a)
		if a == "studio" {
			injectedArgs = append(injectedArgs, "-e", testServer.URL)
		}
	}

	cliApp := icli.App()
	for _, c := range cliApp.Commands {
		if c.Name == "studio" {
			for _, sc := range c.Subcommands {
				if sc.Name == "pull" {
					sc.Action = func(ctx *cli.Context) error {
						var err error
						pr, err = studio.NewPullRunner(ctx, "1.2.3", "justnow", "aaa", "bbb", studio.OptSetNowFn(nowFn))
						require.NoError(t, err)
						return nil
					}
				}
			}
		}
	}
	require.NoError(t, cliApp.Run(injectedArgs))
	return
}

type obj = map[string]any
type arr = []any

func jsonRequestEqual(t *testing.T, r *http.Request, exp any) {
	t.Helper()

	resBytes, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	var act any
	require.NoError(t, json.Unmarshal(resBytes, &act))

	assert.Equal(t, exp, act)
}

func jsonRequestSupersetMatch(t *testing.T, r *http.Request, exp any) {
	t.Helper()

	resBytes, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	expBytes, err := json.Marshal(exp)
	require.NoError(t, err)

	jdopts := jsondiff.DefaultConsoleOptions()
	diff, explanation := jsondiff.Compare(resBytes, expBytes, &jdopts)
	if diff != jsondiff.FullMatch && diff != jsondiff.SupersetMatch {
		t.Errorf("json mismatch:\n%v", explanation)
	}
}

func jsonResponse(t *testing.T, w http.ResponseWriter, res any) {
	t.Helper()

	resBytes, err := json.Marshal(res)
	require.NoError(t, err)

	w.Header().Add("Content-Type", "application/json")
	_, err = w.Write(resBytes)
	require.NoError(t, err)
}

func stringResponse(t *testing.T, w http.ResponseWriter, res string) {
	t.Helper()
	_, err := w.Write([]byte(res))
	require.NoError(t, err)
}

func replacePaths(tmpDir, conf string) string {
	return strings.NewReplacer("$DIR", tmpDir).Replace(conf)
}

func TestPullRunnerHappyPath(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":   "depaid",
				"deployment_name": "Deployment A",
				"main_config":     obj{"name": "main a.yaml", "modified": 1001},
				"resource_configs": arr{
					obj{"name": "resa.yaml", "modified": 1002},
				},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/main%20a.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, `
http:
  enabled: false
input:
  # Needs to keep generating across resource changes but not so much that we
  # swamp the disk with data.
  generate:
    count: 300
    interval: 100ms
    mapping: 'root.id = "first"'
output:
  resource: aoutput
`)
		}),
		expectedRequest("/api/v1/node/session/foosession/download/resa.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "HEAD", r.Method)
		}),
		expectedRequest("/api/v1/node/session/foosession/download/resa.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
output_resources:
  - label: aoutput
    file:
      codec: lines
      path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestSupersetMatch(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "main a.yaml", "modified": 1001.0},
				"resource_configs": arr{
					obj{"name": "resa.yaml", "modified": 1002.0},
				},
			})
			jsonResponse(t, w, obj{
				"add_resources": arr{
					obj{"name": "resa.yaml", "modified": 1003},
				},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/resa.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
output_resources:
  - label: aoutput
    file:
      codec: lines
      path: $DIR/outb.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outb.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*30, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerBadConfig(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  blahbluh:
    nah: nope
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestSupersetMatch(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
				"run_error":   "failed bootstrap config read: maina.yaml: (5,1) unable to infer input type from candidates: [blahbluh]",
			})
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestSupersetMatch(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
				"run_error":   "failed bootstrap config read: maina.yaml: (5,1) unable to infer input type from candidates: [blahbluh]",
			})
			jsonResponse(t, w, obj{
				"main_config": obj{"name": "maina.yaml", "modified": 1002},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	pr.Sync(ctx)
	pr.Sync(ctx)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*10, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerBlockedShutdown(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  retry:
    output:
      http_client:
        url: http://example.com:1234 ]
shutdown_timeout: 2s
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
			})
			jsonResponse(t, w, obj{
				"main_config": obj{"name": "maina.yaml", "modified": 1002},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	time.Sleep(time.Millisecond * 100)
	pr.Sync(ctx)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*10, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerSetOverride(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--set", `input.generate.mapping=root.id = "second"`, "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"second"}`)
	}, time.Second*30, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerReassignment(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestSupersetMatch(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
			})
			jsonResponse(t, w, obj{
				"reassignment": obj{
					"id":   "depbid",
					"name": "Deployment B",
				},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depbid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestSupersetMatch(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
			})
			jsonResponse(t, w, obj{
				"main_config": obj{"name": "mainb.yaml", "modified": 1002.0},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/mainb.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "second"'
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"second"}`)
	}, time.Second*30, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerBaseConfig(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "diskmain.yaml"), []byte(replacePaths(tmpDir, `
http:
  enabled: false
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`)), 0o644))

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "-c", filepath.Join(tmpDir, "diskmain.yaml"), "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*5, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerBaseConfigAndSet(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "diskmain.yaml"), []byte(replacePaths(tmpDir, `
http:
  enabled: false
output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`)), 0o644))

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "-c", filepath.Join(tmpDir, "diskmain.yaml"), "--set", `input.generate.mapping=root.id = "second"`, "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"second"}`)
	}, time.Second*30, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerMetrics(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	tNow := time.Unix(1, 0)
	var tNowMut sync.Mutex

	pr, waitFn := testServerForPullRunner(t, func() time.Time {
		tNowMut.Lock()
		defer tNowMut.Unlock()
		return tNow
	},
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 10,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 10
    interval: ""
    mapping: 'root.id = "first"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
			}) // No metrics yet
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
				"metrics": obj{
					"input": obj{
						"": obj{"received": 10.0},
					},
					"output": obj{
						"aoutput": obj{
							"error": 0.0,
							"sent":  10.0,
						},
					},
					"processor": obj{},
				},
			}) // Metrics sent on flush
			jsonResponse(t, w, obj{
				"main_config": obj{"name": "maina.yaml", "modified": 1002.0},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 5
    interval: ""
    mapping: 'root.id = "second"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outb.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1002.0},
			}) // No metrics again
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1002.0},
				"metrics": obj{
					"input": obj{
						"": obj{"received": 5.0},
					},
					"output": obj{
						"aoutput": obj{
							"error": 0.0,
							"sent":  5.0,
						},
					},
					"processor": obj{},
				},
			}) // Metrics sent on second flush
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	exp := strings.Repeat(`{"id":"first"}`+"\n", 10)
	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return string(data) == exp
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx)

	tNowMut.Lock()
	tNow = time.Unix(12, 0)
	tNowMut.Unlock()
	pr.Sync(ctx)

	exp = strings.Repeat(`{"id":"second"}`+"\n", 5)
	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outb.jsonl"))
		return string(data) == exp
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx)

	tNowMut.Lock()
	tNow = time.Unix(23, 0)
	tNowMut.Unlock()
	pr.Sync(ctx)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerRateLimit(t *testing.T) {
	t.Skip("This test blocks for 1s so dont run by default")

	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	tNow := time.Unix(1, 0)
	var tNowMut sync.Mutex

	pr, waitFn := testServerForPullRunner(t, func() time.Time {
		tNowMut.Lock()
		defer tNowMut.Unlock()
		return tNow
	},
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 10,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
			})
			w.Header().Add("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
			})
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	tStarted := time.Now()
	pr.Sync(ctx)
	tTaken := time.Since(tStarted)

	assert.Greater(t, tTaken, time.Second)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first"}`)
	}, time.Second*30, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerTracesDisabled(t *testing.T) {
	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":   "depaid",
				"deployment_name": "Deployment A",
				"main_config":     obj{"name": "maina.yaml", "modified": 1001},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  label: ainput
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
				"metrics": obj{
					"input":     obj{"ainput": obj{"received": 1.0}},
					"output":    obj{"aoutput": obj{"error": 0.0, "sent": 1.0}},
					"processor": obj{},
				},
			}) // No traces yet
			jsonResponse(t, w, obj{
				"main_config":      obj{"name": "maina.yaml", "modified": 1002.0},
				"requested_traces": 100,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  label: ainput
  generate:
    count: 10
    interval: ""
    mapping: 'root.id = "second"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outb.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1002.0},
				"metrics": obj{
					"input":     obj{"ainput": obj{"received": 10.0}},
					"output":    obj{"aoutput": obj{"error": 0.0, "sent": 10.0}},
					"processor": obj{},
				},
			}) // Metrics sent on flush, still no traces as it's not enabled
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	exp := strings.Repeat(`{"id":"first"}`+"\n", 1)
	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return string(data) == exp
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx) // Includes new config and requests traces

	exp = strings.Repeat(`{"id":"second"}`+"\n", 10)
	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outb.jsonl"))
		return string(data) == exp
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx) // Provides were requested above

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerTracesEnabled(t *testing.T) {
	t.Skip("Traces are non-deterministic so we need to rework the sync response check")

	tmpDir := t.TempDir()

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession", "--send-traces"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":   "depaid",
				"deployment_name": "Deployment A",
				"main_config":     obj{"name": "maina.yaml", "modified": 1001},
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  label: ainput
  generate:
    count: 1
    interval: ""
    mapping: 'root.id = "first"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1001.0},
				"metrics": obj{
					"input":     obj{"ainput": obj{"received": 1.0}},
					"output":    obj{"aoutput": obj{"error": 0.0, "sent": 1.0}},
					"processor": obj{},
				},
			}) // No traces yet
			jsonResponse(t, w, obj{
				"main_config":      obj{"name": "maina.yaml", "modified": 1002.0},
				"requested_traces": 100,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false
input:
  label: ainput
  generate:
    count: 10
    interval: ""
    mapping: 'root.id = "second"'
output:
  label: aoutput
  file:
    codec: lines
    path: $DIR/outb.jsonl
`))
		}),
		expectedRequest("/api/v1/node/session/foosession/deployment/depaid/sync", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name":        "foobarnode",
				"main_config": obj{"name": "maina.yaml", "modified": 1002.0},
				"metrics": obj{
					"input":     obj{"ainput": obj{"received": 10.0}},
					"output":    obj{"aoutput": obj{"error": 0.0, "sent": 10.0}},
					"processor": obj{},
				},
				"tracing": obj{
					"input_events": obj{
						"ainput": arr{
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "PRODUCE"},
						},
					},
					"output_events": obj{
						"aoutput": arr{
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
							obj{"content": "{\"id\":\"second\"}", "metadata": obj{}, "type": "CONSUME"},
						},
					},
				},
			}) // Metrics sent on flush
			jsonResponse(t, w, obj{})
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	exp := strings.Repeat(`{"id":"first"}`+"\n", 1)
	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return string(data) == exp
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx) // Includes new config and requests traces

	exp = strings.Repeat(`{"id":"second"}`+"\n", 10)
	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outb.jsonl"))
		return string(data) == exp
	}, time.Second*30, time.Millisecond*10)

	pr.Sync(ctx) // Provides traces from above writes

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}

func TestPullRunnerSharedMappings(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "a.blobl"), []byte(`
map a {
  root.id = this.id + " and a"
}
`), 0o755))

	ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
	defer done()

	pr, waitFn := testServerForPullRunner(t, nil,
		[]string{"benthos", "--log.level", "none", "studio", "pull", "--name", "foobarnode", "--session", "foosession"},
		expectedRequest("/api/v1/node/session/foosession/init", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
			jsonRequestEqual(t, r, obj{
				"name": "foobarnode",
			})
			jsonResponse(t, w, obj{
				"deployment_id":                "depaid",
				"deployment_name":              "Deployment A",
				"main_config":                  obj{"name": "maina.yaml", "modified": 1001},
				"metrics_guide_period_seconds": 300,
			})
		}),
		expectedRequest("/api/v1/node/session/foosession/download/maina.yaml", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, replacePaths(tmpDir, `
http:
  enabled: false

input:
  # Needs to keep generating across resource changes but not so much that we
  # swamp the disk with data.
  generate:
    count: 300
    interval: 100ms
    mapping: |
      import "$DIR/a.blobl"
      import "./b.blobl"

      root = {"id":"first"}.apply("a").apply("b")

output:
  file:
    codec: lines
    path: $DIR/outa.jsonl
`))
		}),
		expectedRequest(
			fmt.Sprintf("/api/v1/node/session/foosession/download/%v", url.PathEscape(filepath.Join(tmpDir, "a.blobl"))),
			func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				http.Error(w, "Nah", http.StatusNotFound)
			}),
		expectedRequest("/api/v1/node/session/foosession/download/b.blobl", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, `
map b {
  root.id = this.id + " and b"
}
`)
		}),
		expectedRequest(
			fmt.Sprintf("/api/v1/node/session/foosession/download/%v", url.PathEscape(filepath.Join(tmpDir, "a.blobl"))),
			func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				http.Error(w, "Nah", http.StatusNotFound)
			}),
		expectedRequest("/api/v1/node/session/foosession/download/b.blobl", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "GET", r.Method)
			stringResponse(t, w, `
map b {
  root.id = this.id + " and b"
}
`)
		}),
		expectedRequest("/api/v1/node/session/foosession/leave", func(t *testing.T, w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "POST", r.Method)
		}),
	)

	assert.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(tmpDir, "outa.jsonl"))
		return strings.Contains(string(data), `{"id":"first and a and b"}`)
	}, time.Second*10, time.Millisecond*10)

	require.NoError(t, pr.Stop(ctx))
	waitFn(ctx)
}
