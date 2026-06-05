// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package db2test provides helpers for integration tests against a live IBM DB2
// instance running in a Docker container.
package db2test

import (
	"archive/tar"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	dockerclient "github.com/docker/docker/client"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/connect/v4/internal/impl/db2/db2cli"
)

const (
	// db2Image is the IBM DB2 Community Edition image.  It only publishes
	// linux/amd64 manifests, so Apple Silicon hosts must use Rosetta emulation.
	db2Image    = "icr.io/db2_community/db2:latest"
	db2Port     = "50000/tcp"
	db2Database = "TESTDB"
	db2User     = "db2inst1"
	db2Password = "password"

	// db2BaseDir is the root of the DB2 installation inside the container and on
	// the test host. DB2 CLI reads the DB2DIR environment variable (which the test
	// runner sets to this value) to locate auxiliary files such as message
	// catalogs and the driver configuration.
	db2BaseDir = "/opt/ibm/db2/V12.1"

	// db2LibDir is the DB2 CLI library directory inside the container. The entire
	// directory is extracted because libdb2.so.1 depends on other IBM-specific
	// libraries (libdb2osse.so.1, ICU libs, etc.) that are only present inside
	// the DB2 image — not on the test host.
	db2LibDir = db2BaseDir + "/lib64"

	// db2LibInstallDir is where extracted DB2 libraries are placed. We use the
	// same absolute path as inside the DB2 container because libdb2.so.1 and its
	// dependencies are compiled with RPATH=/opt/ibm/db2/V12.1/lib64. If the libs
	// are placed in a different directory (e.g. /usr/local/lib), the dynamic
	// linker resolves libdb2.so.1 fine but then fails to find its dependencies
	// because RPATH still points to the original path. Using the same path keeps
	// RPATH resolution correct.
	db2LibInstallDir = db2BaseDir + "/lib64"

	// db2MsgDir is the message catalog directory inside the container. DB2 CLI
	// loads message catalogs from $DB2DIR/msg/ to format human-readable errors.
	// Without these files every CLI error is replaced by SQL10007N, which masks
	// the real connection failure.
	db2MsgDir = db2BaseDir + "/msg"

	// db2LdConfFile is the ldconfig drop-in that registers db2LibInstallDir so
	// that dlopen("libdb2.so.1") works without specifying a full path.
	db2LdConfFile = "/etc/ld.so.conf.d/db2.conf"
)

// TestDB wraps *sql.DB with test-oriented helpers for DB2 integration tests.
// It embeds *sql.DB so all standard database/sql methods are available. The T
// field provides test-failure reporting and the DSN field stores the connection
// string used to create the underlying connection, which is useful when tests
// need to open additional connections.
type TestDB struct {
	*sql.DB
	T   *testing.T
	DSN string
}

// MustExec executes a SQL statement and calls t.Fatalf if it returns an error.
// Use this for DDL and DML statements in test setup/teardown where a failure
// means the test cannot continue meaningfully.
func (db *TestDB) MustExec(query string, args ...any) {
	_, err := db.Exec(query, args...)
	require.NoError(db.T, err, "MustExec: %s", query)
}

// MustExecContext executes a SQL statement with a context and calls t.Fatalf
// if it returns an error. Use this variant when the test already has a context
// with a deadline (e.g. from t.Context()) to propagate cancellation.
func (db *TestDB) MustExecContext(ctx context.Context, query string, args ...any) {
	_, err := db.ExecContext(ctx, query, args...)
	require.NoError(db.T, err, "MustExecContext: %s", query)
}

// db2LibOnce ensures the DB2 CLI library is extracted from the container and
// loaded into the process exactly once, even when tests run in parallel.
var (
	db2LibOnce sync.Once
	db2LibErr  error
)

// sharedContainerOnce guards the package-level container singleton so that all
// integration tests in this package share a single DB2 container.  Starting two
// containers sequentially (one per test) each takes 5–10 min and easily exceeds
// the 20-minute test timeout when running under DinD on macOS.
var (
	sharedContainerOnce sync.Once
	sharedContainer     testcontainers.Container
	sharedDSN           string
	sharedContainerErr  error
)

// darwinContainerName holds the name of the externally-managed DB2 container
// when running on macOS in native mode (DB2_DARWIN_CONTAINER env var).  Shell
// commands that would normally run as db2inst1 inside the container are
// dispatched via "docker exec {darwinContainerName} su - db2inst1 -c {cmd}".
var darwinContainerName string

// db2LogConsumer streams DB2 container stdout/stderr to the test process stdout
// so startup progress is visible during the ~5–8 minute first-time setup.
type db2LogConsumer struct{}

func (*db2LogConsumer) Accept(l testcontainers.Log) {
	fmt.Printf("[db2-container] %s", l.Content)
}

// findDarwinLib searches for libdb2.dylib (IBM DB2 ODBC CLI Driver / clidriver) on macOS.
// The clidriver connects via TCP/IP only — no SysV IPC, no --ipc=host required.
// Returns the absolute path if found, or an error with installation instructions.
func findDarwinLib() (string, error) {
	if p := os.Getenv("DB2_DYLIB_PATH"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
		return "", fmt.Errorf("DB2_DYLIB_PATH=%q: file not found", p)
	}
	// ibm_db Python package bundles the clidriver
	out, err := exec.Command("python3", "-c",
		"import ibm_db, os; print(os.path.dirname(ibm_db.__file__))").Output()
	if err == nil {
		c := filepath.Join(strings.TrimSpace(string(out)), "clidriver", "lib", "libdb2.dylib")
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	for _, c := range []string{
		"/Library/IBM/SQLLIB/lib64/libdb2.dylib",
		"/Library/IBM/SQLLIB/lib/libdb2.dylib",
		filepath.Join(os.Getenv("HOME"), "sqllib", "lib64", "libdb2.dylib"),
	} {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", errors.New(`IBM DB2 ODBC CLI Driver (libdb2.dylib) not found.

Install options:
  pip3 install ibm_db            # bundles clidriver for macOS
  export DB2_DYLIB_PATH=/path/to/libdb2.dylib

See: https://www.ibm.com/support/pages/db2-odbc-cli-driver-download-and-installation-information`)
}

// acquireDarwinDB2 loads the IBM clidriver dylib and connects to the DB2 container
// on the host/port given by DB2_DARWIN_HOST / DB2_DARWIN_PORT (defaults: localhost:50000).
// The clidriver is TCP-only — no SysV IPC, so --ipc=host is not needed on macOS.
func acquireDarwinDB2(ctx context.Context) {
	dylibPath, err := findDarwinLib()
	if err != nil {
		sharedContainerErr = err
		return
	}
	if err := db2cli.LoadLibraryFromPath(dylibPath); err != nil {
		sharedContainerErr = fmt.Errorf("loading libdb2.dylib from %s: %w", dylibPath, err)
		return
	}

	host := os.Getenv("DB2_DARWIN_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("DB2_DARWIN_PORT")
	if port == "" {
		port = "50000"
	}

	dsn := fmt.Sprintf(
		"DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s",
		db2Database, host, port, db2User, db2Password,
	)

	fmt.Printf("db2test: macOS native mode — connecting via clidriver to %s:%s (up to 6 min)...\n", host, port)
	deadline := time.Now().Add(6 * time.Minute)
	for {
		db, openErr := sql.Open("db2-cli", dsn)
		if openErr == nil {
			db.SetMaxOpenConns(1)
			pingErr := db.PingContext(ctx)
			_ = db.Close()
			if pingErr == nil {
				break
			}
			fmt.Printf("db2test: DB2 not ready yet (%v), retrying...\n", pingErr)
		}
		if time.Now().After(deadline) {
			sharedContainerErr = errors.New("DB2 did not accept connections within 6 min (macOS native mode)")
			return
		}
		time.Sleep(5 * time.Second)
	}

	sharedDSN = dsn
	fmt.Printf("db2test: DB2 accepting connections (macOS native, clidriver at %s)\n", dylibPath)

	// Set the container name so that runAsDB2Inst1 / findAsnScript / startAsncapLocal
	// dispatch through "docker exec" rather than running shell commands locally.
	darwinContainerName = os.Getenv("DB2_DARWIN_CONTAINER")

	tuneDB, openErr := sql.Open("db2-cli", dsn)
	if openErr == nil {
		defer tuneDB.Close()
		tuneDB.SetMaxOpenConns(1)
		stmt := fmt.Sprintf("CALL SYSPROC.ADMIN_CMD('update db cfg for %s using LOCKTIMEOUT 30')", db2Database)
		if _, err := tuneDB.ExecContext(ctx, stmt); err != nil {
			fmt.Printf("db2test: DB tuning warning: %v\n", err)
		} else {
			fmt.Printf("db2test: LOCKTIMEOUT set to 30 s\n")
		}
	}

	// Create ASNCDC control tables and start the capture daemon — same as
	// acquireLocalDB2.  Shell commands are dispatched via docker exec because
	// the IBM SQL scripts and asncap binary only exist inside the container.
	if darwinContainerName == "" {
		fmt.Printf("db2test: DB2_DARWIN_CONTAINER not set — ASNCDC setup skipped (CDC tests will fail)\n")
		return
	}
	setupDB, openErr2 := sql.Open("db2-cli", dsn)
	if openErr2 != nil {
		fmt.Printf("db2test: ASNCDC setup skipped (open: %v)\n", openErr2)
		return
	}
	defer setupDB.Close()
	setupDB.SetMaxOpenConns(1)

	if err := createASNCDCTablesIfNeeded(ctx, setupDB); err != nil {
		fmt.Printf("db2test: ASNCDC table setup warning (non-fatal): %v\n", err)
	}
	if err := startAsncapLocal(); err != nil {
		fmt.Printf("db2test: asncap start warning (non-fatal): %v\n", err)
	}
}

// AcquireSharedContainer starts (or returns the already-running) DB2 container.
// Safe to call concurrently; the container is started at most once per process.
// Call TerminateSharedContainer from TestMain after m.Run() to clean up.
//
// If DB2_CONTAINER_NAME is set in the environment the container is given that
// name. The run-db2-macos-integration-tests.sh script sets this to a unique
// per-run name so it can tail logs and clean up by name without touching other
// containers.
//
// If DB2_USE_LOCAL=1 is set the function skips container creation and
// connects directly to a DB2 instance already running at 127.0.0.1:50000
// (the local DB2 in the container the test binary is executing inside).
// The run-db2-macos-integration-tests.sh script uses this mode by copying the
// test binary into the DB2 container and running it there, which avoids all
// IPC-namespace and library-extraction problems caused by libdb2.so.1 (the
// full server library) trying to initialise a local DB2 engine in a separate
// container where no DB2 instance is running.
//
// On macOS (darwin), the clidriver path is used instead: the test binary runs
// natively on the host and connects via TCP to the mapped port. Set DB2_DYLIB_PATH
// or install ibm_db (pip3 install ibm_db) to provide libdb2.dylib.
func AcquireSharedContainer(ctx context.Context) (testcontainers.Container, string, error) {
	sharedContainerOnce.Do(func() {
		if runtime.GOOS == "darwin" {
			acquireDarwinDB2(ctx)
			return
		}
		if os.Getenv("DB2_USE_LOCAL") == "1" {
			acquireLocalDB2(ctx)
			return
		}
		fmt.Printf("db2test: starting DB2 container (first-time setup ~5–8 min)...\n")
		opts := []testcontainers.ContainerCustomizer{
			testcontainers.WithImagePlatform("linux/amd64"),
			testcontainers.WithEnv(map[string]string{
				"LICENSE":           "accept",
				"DB2INST1_PASSWORD": db2Password,
				"DBNAME":            db2Database,
				// AUTOCONFIG=false: skip DB2 hardware auto-discovery (saves 2-3 min).
				"AUTOCONFIG": "false",
				// ARCHIVE_LOGS=true: enable transaction log archiving required for CDC.
				// The init script sets LOGARCHMETH1=LOGRETAIN and restarts the DB; the
				// "DEACTIVATED: NO" log message fires when that restart completes, which
				// is our secondary wait-strategy trigger below.
				"ARCHIVE_LOGS": "true",
				// SAMPLEDB=false: skip IBM sample database creation (saves ~1 min).
				"SAMPLEDB": "false",
				// REPODB=false: skip DSM repository database creation (saves ~1 min).
				// This is a second full CREATE DATABASE call; we do not use the IBM
				// Data Server Manager console in tests.
				"REPODB": "false",
				// HADR_ENABLED=NO: skip High-Availability Disaster Recovery setup.
				"HADR_ENABLED": "NO",
				// UPDATEAVAIL=NO: skip update-availability check at startup.
				"UPDATEAVAIL": "NO",
			}),
			testcontainers.WithExposedPorts(db2Port),
			testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
				hc.Privileged = true
				// DB2's IPC between db2agent and db2fmp uses shared memory. Docker's
				// default /dev/shm of 64 MB is too small; 512 MB prevents transient
				// SQL0902C / shared-memory exhaustion errors under load.
				hc.ShmSize = 512 * 1024 * 1024
				// Share the host IPC namespace so the test runner container (also
				// started with --ipc=host) can attach to the SysV shared memory
				// segments that libdb2.so.1 initialises in this container.
				// db2systm stores the IPC keys; without a shared namespace the keys
				// are invisible from the test runner and every SQLDriverConnect
				// returns SQL1042C.
				hc.IpcMode = "host"
			}),
			testcontainers.WithLogConsumers(&db2LogConsumer{}),
			// Wait for the DB2 init script to finish. "Setup has completed" is
			// printed at the very end of the entrypoint after the full sequence:
			// CREATE DATABASE → LOGARCHMETH1=LOGRETAIN → backup → restart.
			// The connection-retry loop below handles any remaining delay before
			// TCPIP connections are accepted. 20-minute deadline gives ample room
			// for slow emulated hardware (Apple Silicon / Docker Desktop).
			testcontainers.WithWaitStrategyAndDeadline(20*time.Minute,
				wait.ForLog("Setup has completed").WithStartupTimeout(20*time.Minute),
			),
		}
		if name := os.Getenv("DB2_CONTAINER_NAME"); name != "" {
			opts = append(opts, testcontainers.WithName(name))
		}
		ctr, err := testcontainers.Run(ctx, db2Image, opts...)
		if err != nil {
			sharedContainerErr = fmt.Errorf("starting shared DB2 container: %w", err)
			return
		}
		sharedContainer = ctr
		fmt.Printf("db2test: container up, extracting DB2 libraries...\n")

		if err := ensureDB2Library(ctx, ctr); err != nil {
			sharedContainerErr = fmt.Errorf("loading DB2 library: %w", err)
			return
		}

		// Prefer a direct container-to-container connection on the Docker bridge
		// network (container IP : 50000) over the host.docker.internal:mapped_port
		// NAT route.  The NAT path causes SQL1042C on macOS Docker Desktop because
		// packets go: test-runner → host-gateway → port-mapping → DB2 container,
		// and DB2's TCPIP listener returns SQL1042C when the originating IP is the
		// gateway rather than a local peer.  Direct bridge IP bypasses this.
		dsn := containerDirectDSN(ctx, ctr)
		if dsn == "" {
			// Fallback: use testcontainers-provided host + mapped port.
			host, hostErr := ctr.Host(ctx)
			if hostErr != nil {
				sharedContainerErr = fmt.Errorf("getting container host: %w", hostErr)
				return
			}
			port, portErr := ctr.MappedPort(ctx, db2Port)
			if portErr != nil {
				sharedContainerErr = fmt.Errorf("getting container port: %w", portErr)
				return
			}
			dsn = fmt.Sprintf(
				"DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s",
				db2Database, host, port.Port(), db2User, db2Password,
			)
			fmt.Printf("db2test: using fallback DSN (host.docker.internal): %s\n", host)
		}

		// "Setup has completed" fires when the init script finishes, but DB2
		// needs additional time to start accepting TCPIP connections.  Verify
		// the connection before declaring the container ready so that SetupTest
		// never races against this window.
		// DB2 returns SQL1042C for several minutes after "Setup has completed"
		// while its TCPIP listener finishes initialising post-restart.  Allow
		// up to 6 minutes; on fast hardware it usually clears in ~2–3 min.
		fmt.Printf("db2test: verifying DB2 connection (up to 6 min)...\n")
		deadline := time.Now().Add(6 * time.Minute)
		for {
			db, openErr := sql.Open("db2-cli", dsn)
			if openErr == nil {
				db.SetMaxOpenConns(1)
				pingErr := db.PingContext(ctx)
				_ = db.Close()
				if pingErr == nil {
					break
				}
				fmt.Printf("db2test: DB2 not ready yet (%v), retrying...\n", pingErr)
			}
			if time.Now().After(deadline) {
				sharedContainerErr = errors.New("DB2 did not accept connections within 6 min")
				return
			}
			time.Sleep(5 * time.Second)
		}

		sharedDSN = dsn
		fmt.Printf("db2test: DB2 accepting connections\n")

		// Apply test-specific DB tuning. LOCKTIMEOUT prevents tests from hanging
		// indefinitely on lock contention; the default is -1 (wait forever).
		// SYSPROC.ADMIN_CMD is used because UPDATE DATABASE CONFIGURATION is a
		// DB2 command-line command, not an SQL statement.
		func() {
			tuneDB, openErr := sql.Open("db2-cli", dsn)
			if openErr != nil {
				fmt.Printf("db2test: DB tuning skipped (open: %v)\n", openErr)
				return
			}
			defer tuneDB.Close()
			tuneDB.SetMaxOpenConns(1)
			stmt := fmt.Sprintf("CALL SYSPROC.ADMIN_CMD('update db cfg for %s using LOCKTIMEOUT 30')", db2Database)
			if _, err := tuneDB.ExecContext(ctx, stmt); err != nil {
				fmt.Printf("db2test: DB tuning warning: %v\n", err)
			} else {
				fmt.Printf("db2test: LOCKTIMEOUT set to 30 s\n")
			}
		}()
	})
	return sharedContainer, sharedDSN, sharedContainerErr
}

// TerminateSharedContainer stops the shared DB2 container.
// Call from TestMain after m.Run() to clean up.
func TerminateSharedContainer(ctx context.Context) {
	if sharedContainer != nil {
		_ = sharedContainer.Terminate(ctx)
	}
}

// acquireLocalDB2 sets up the shared DSN for a DB2 instance already running
// locally at 127.0.0.1:50000.  Called from AcquireSharedContainer when
// DB2_USE_LOCAL=1 is set (i.e. the test binary is running inside the DB2
// container via docker exec from run-db2-macos-integration-tests.sh).
func acquireLocalDB2(ctx context.Context) {
	fmt.Printf("db2test: local mode — using DB2 instance at 127.0.0.1:50000\n")

	// Library is already present at the standard path inside the container.
	if err := ensureDB2Library(ctx, nil); err != nil {
		sharedContainerErr = fmt.Errorf("loading DB2 library (local mode): %w", err)
		return
	}

	dsn := fmt.Sprintf(
		"DATABASE=%s;HOSTNAME=127.0.0.1;PORT=50000;PROTOCOL=TCPIP;UID=%s;PWD=%s",
		db2Database, db2User, db2Password,
	)

	fmt.Printf("db2test: verifying local DB2 connection (up to 2 min)...\n")
	deadline := time.Now().Add(2 * time.Minute)
	for {
		db, openErr := sql.Open("db2-cli", dsn)
		if openErr == nil {
			db.SetMaxOpenConns(1)
			pingErr := db.PingContext(ctx)
			_ = db.Close()
			if pingErr == nil {
				break
			}
			fmt.Printf("db2test: DB2 not ready yet (%v), retrying...\n", pingErr)
		}
		if time.Now().After(deadline) {
			sharedContainerErr = errors.New("local DB2 did not accept connections within 2 min")
			return
		}
		time.Sleep(5 * time.Second)
	}

	sharedDSN = dsn
	fmt.Printf("db2test: DB2 accepting connections (local mode)\n")

	// Tune LOCKTIMEOUT via shell — UPDATE DATABASE CONFIGURATION is a DB2 CLI
	// command that SYSPROC.ADMIN_CMD may reject depending on the privilege level.
	if out, err := runAsDB2Inst1(fmt.Sprintf(
		"db2 update db cfg for %s using LOCKTIMEOUT 30", db2Database,
	)); err != nil {
		fmt.Printf("db2test: LOCKTIMEOUT warning: %v\n%s\n", err, out)
	} else {
		fmt.Printf("db2test: LOCKTIMEOUT set to 30 s\n")
	}

	// Create ASNCDC control tables and start the capture daemon.  This must
	// happen here (once, before any test calls EnableASNCDC) because the setup
	// involves running IBM SQL scripts and the asncap binary, both of which
	// require the db2inst1 environment and must not run in parallel.
	setupDB, openErr := sql.Open("db2-cli", dsn)
	if openErr != nil {
		fmt.Printf("db2test: ASNCDC setup skipped (open: %v)\n", openErr)
		return
	}
	defer setupDB.Close()
	setupDB.SetMaxOpenConns(1)

	if err := createASNCDCTablesIfNeeded(ctx, setupDB); err != nil {
		fmt.Printf("db2test: ASNCDC table setup warning (non-fatal): %v\n", err)
	}
	if err := startAsncapLocal(); err != nil {
		fmt.Printf("db2test: asncap start warning (non-fatal): %v\n", err)
	}
}

// ensureDB2Library extracts the DB2 CLI shared libraries from the running
// container into /opt/ibm/db2/V12.1/lib64 (the same path used inside the DB2
// image), writes a drop-in ldconfig config file, runs ldconfig, and calls
// db2cli.LoadLibrary so the purego driver can dlopen them.
//
// We copy the entire lib64 directory (not just libdb2.so.1) because the main
// library depends on IBM-specific shared objects — libdb2osse.so.1, ICU libs,
// etc. — that are only available inside the DB2 container image.
//
// LD_LIBRARY_PATH cannot be used here: in a pure-Go binary (no CGO),
// os.Setenv updates Go's internal env cache but does NOT call the C library's
// setenv(3), so dlopen (a C function) never sees the change. The solution is
// to extract the libs to the SAME path they lived in inside the container
// (/opt/ibm/db2/V12.1/lib64) — libdb2.so.1 is compiled with RPATH pointing
// to that directory, so placing the files there keeps transitive dependency
// resolution correct. A drop-in ldconfig config file then registers the
// directory so dlopen("libdb2.so.1") resolves by name.
//
// The extraction runs exactly once per process (via sync.Once); parallel tests
// share the result safely.
func ensureDB2Library(ctx context.Context, ctr testcontainers.Container) error {
	db2LibOnce.Do(func() {
		if ctr == nil {
			// Local mode: the library is already installed inside the DB2 container
			// at the standard path.  Register it with ldconfig and dlopen it.
			if mkErr := os.MkdirAll("/etc/ld.so.conf.d", 0o755); mkErr == nil {
				_ = os.WriteFile(db2LdConfFile, []byte(db2LibInstallDir+"\n"), 0o644)
				if out, ldErr := exec.Command("ldconfig").CombinedOutput(); ldErr != nil {
					fmt.Printf("db2test: ldconfig warning: %v\n%s\n", ldErr, out)
				}
			}
			libPath := filepath.Join(db2LibInstallDir, "libdb2.so.1")
			fmt.Printf("db2test: loading DB2 library from %s (local mode)\n", libPath)
			db2LibErr = db2cli.LoadLibraryFromPath(libPath)
			if db2LibErr != nil {
				return
			}
			var warmupHenv db2cli.SQLHENV
			if r := db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(db2cli.SQL_NULL_HENV), (*db2cli.SQLHANDLE)(&warmupHenv)); r == db2cli.SQL_SUCCESS || r == db2cli.SQL_SUCCESS_WITH_INFO {
				db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(warmupHenv))
			}
			fmt.Printf("db2test: DB2 CLI library warm-up complete (local mode)\n")
			return
		}

		// testcontainers' CopyFileFromContainer is designed for single files only — it
		// internally calls tarReader.Next() once, advancing past the directory header
		// and leaving the reader at EOF for a directory path. Use the Docker client
		// directly to get the raw unmodified tar stream for the lib64 directory.
		dc, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
		if err != nil {
			db2LibErr = fmt.Errorf("creating Docker client: %w", err)
			return
		}
		defer dc.Close()

		tarStream, _, err := dc.CopyFromContainer(ctx, ctr.GetContainerID(), db2LibDir)
		if err != nil {
			db2LibErr = fmt.Errorf("copy %s from container: %w", db2LibDir, err)
			return
		}
		defer tarStream.Close()

		if err := os.MkdirAll(db2LibInstallDir, 0o755); err != nil {
			db2LibErr = fmt.Errorf("create %s: %w", db2LibInstallDir, err)
			return
		}

		// Extract .so files and their symlinks into db2LibInstallDir.
		tr := tar.NewReader(tarStream)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				db2LibErr = fmt.Errorf("reading DB2 lib64 tar: %w", err)
				return
			}

			name := filepath.Base(hdr.Name)
			if !strings.Contains(name, ".so") {
				continue
			}

			destPath := filepath.Join(db2LibInstallDir, name)

			switch hdr.Typeflag {
			case tar.TypeSymlink:
				// Rewrite the link target to its basename so it resolves within
				// the flat install dir rather than pointing into the container path.
				target := filepath.Base(hdr.Linkname)
				if linkErr := os.Symlink(target, destPath); linkErr != nil && !os.IsExist(linkErr) {
					db2LibErr = fmt.Errorf("symlink %s → %s: %w", destPath, target, linkErr)
					return
				}
			case tar.TypeReg:
				f, createErr := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)|0o755)
				if createErr != nil {
					db2LibErr = fmt.Errorf("create %s: %w", destPath, createErr)
					return
				}
				if _, copyErr := io.Copy(f, tr); copyErr != nil {
					f.Close()
					db2LibErr = fmt.Errorf("write %s: %w", destPath, copyErr)
					return
				}
				f.Close()
			}
		}

		// Extract message catalog directory ($DB2DIR/msg/) from the container.
		// DB2 CLI loads .mo/.cat files from this directory to format error messages.
		// Without them every error is replaced with SQL10007N, masking the real cause.
		msgTarStream, _, err := dc.CopyFromContainer(ctx, ctr.GetContainerID(), db2MsgDir)
		if err != nil {
			db2LibErr = fmt.Errorf("copy %s from container: %w", db2MsgDir, err)
			return
		}
		defer msgTarStream.Close()

		msgTR := tar.NewReader(msgTarStream)
		for {
			hdr, err := msgTR.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				db2LibErr = fmt.Errorf("reading DB2 msg tar: %w", err)
				return
			}
			// Tar entries have paths like "msg/en_US.iso88591/db2adm.cat".
			// Prepend db2BaseDir to reconstruct the full installation path.
			destPath := filepath.Join(db2BaseDir, hdr.Name)
			switch hdr.Typeflag {
			case tar.TypeDir:
				if mkErr := os.MkdirAll(destPath, 0o755); mkErr != nil {
					db2LibErr = fmt.Errorf("create dir %s: %w", destPath, mkErr)
					return
				}
			case tar.TypeReg:
				if mkErr := os.MkdirAll(filepath.Dir(destPath), 0o755); mkErr != nil {
					db2LibErr = fmt.Errorf("create parent for %s: %w", destPath, mkErr)
					return
				}
				f, createErr := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)|0o644)
				if createErr != nil {
					db2LibErr = fmt.Errorf("create %s: %w", destPath, createErr)
					return
				}
				if _, copyErr := io.Copy(f, msgTR); copyErr != nil {
					f.Close()
					db2LibErr = fmt.Errorf("write %s: %w", destPath, copyErr)
					return
				}
				f.Close()
			case tar.TypeSymlink:
				// Catalog files may be versioned symlinks (e.g. db2ca.cat -> db2ca.1.1.cat).
				// Preserve them so DB2 CLI can resolve the canonical names.
				if mkErr := os.MkdirAll(filepath.Dir(destPath), 0o755); mkErr == nil {
					_ = os.Remove(destPath) // overwrite any stale symlink
					_ = os.Symlink(hdr.Linkname, destPath)
				}
			}
		}
		fmt.Printf("db2test: extracted %s OK\n", db2MsgDir)

		// Create locale symlinks in the msg directory so DB2 CLI can find message
		// catalogs regardless of the active LANG setting in the test container.
		// The container locale may differ from en_US.iso88591 which is the only
		// subdirectory extracted from the DB2 image.
		msgInstallDir := filepath.Join(db2BaseDir, "msg")
		for _, alias := range []string{"C", "en_US", "en_US.UTF-8", "en_US.utf8"} {
			linkPath := filepath.Join(msgInstallDir, alias)
			if _, statErr := os.Lstat(linkPath); os.IsNotExist(statErr) {
				_ = os.Symlink("en_US.iso88591", linkPath)
			}
		}

		// Extract DB2 security plugin directory ($DB2DIR/security64/) from the
		// container. DB2 CLI needs the security plugins (IBMOSauthclient.so etc.)
		// to authenticate via username/password over TCPIP. The instance
		// sqllib/security64/plugin/IBM is a symlink to $DB2DIR/security64/plugin/IBM,
		// so extracting the global directory is sufficient.
		sec64Src := db2BaseDir + "/security64"
		if sec64TarStream, _, sec64Err := dc.CopyFromContainer(ctx, ctr.GetContainerID(), sec64Src); sec64Err == nil {
			defer sec64TarStream.Close()
			sec64TR := tar.NewReader(sec64TarStream)
			for {
				hdr, err := sec64TR.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					db2LibErr = fmt.Errorf("reading security64 tar: %w", err)
					return
				}
				destPath := filepath.Join(db2BaseDir, hdr.Name)
				switch hdr.Typeflag {
				case tar.TypeDir:
					if mkErr := os.MkdirAll(destPath, 0o755); mkErr != nil {
						db2LibErr = fmt.Errorf("create dir %s: %w", destPath, mkErr)
						return
					}
				case tar.TypeReg:
					if mkErr := os.MkdirAll(filepath.Dir(destPath), 0o755); mkErr != nil {
						db2LibErr = fmt.Errorf("create parent for %s: %w", destPath, mkErr)
						return
					}
					f, createErr := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)|0o755)
					if createErr != nil {
						db2LibErr = fmt.Errorf("create %s: %w", destPath, createErr)
						return
					}
					if _, copyErr := io.Copy(f, sec64TR); copyErr != nil {
						f.Close()
						db2LibErr = fmt.Errorf("write %s: %w", destPath, copyErr)
						return
					}
					f.Close()
				case tar.TypeSymlink:
					if mkErr := os.MkdirAll(filepath.Dir(destPath), 0o755); mkErr == nil {
						_ = os.Remove(destPath)
						_ = os.Symlink(hdr.Linkname, destPath)
					}
				}
			}
			fmt.Printf("db2test: extracted %s OK\n", sec64Src)
		} else {
			fmt.Printf("db2test: security64 not available (non-fatal): %v\n", sec64Err)
		}

		// Create a minimal db2dsdriver.cfg so the CLI knows it is operating as a
		// remote-only thin client (no local DB2 instance required).
		cfgDir := filepath.Join(db2BaseDir, "cfg")
		if mkErr := os.MkdirAll(cfgDir, 0o755); mkErr != nil {
			db2LibErr = fmt.Errorf("create cfg dir: %w", mkErr)
			return
		}
		dsdriverCfg := filepath.Join(cfgDir, "db2dsdriver.cfg")
		if _, statErr := os.Stat(dsdriverCfg); os.IsNotExist(statErr) {
			if writeErr := os.WriteFile(dsdriverCfg, []byte("<configuration>\n</configuration>\n"), 0o644); writeErr != nil {
				db2LibErr = fmt.Errorf("create db2dsdriver.cfg: %w", writeErr)
				return
			}
		}

		// Populate the DB2 instance home that libdb2.so.1 locates via
		// getpwnam("db2inst1").  In the icr.io/db2_community/db2 image the
		// instance owner's home is /database/config/db2inst1 (not /home/db2inst1).
		// The server library requires db2systm and db2nodes.cfg in sqllib/ to
		// initialise even for remote TCPIP connections.
		instanceHome := "/database/config/db2inst1"
		instanceSqllib := instanceHome + "/sqllib"
		for _, relPath := range []string{
			"db2systm",
			"db2nodes.cfg",
			filepath.Join("cfg", "db2cli.ini"),
		} {
			srcPath := instanceSqllib + "/" + relPath
			dstPath := filepath.Join(instanceSqllib, relPath)
			if mkErr := os.MkdirAll(filepath.Dir(dstPath), 0o755); mkErr != nil {
				db2LibErr = fmt.Errorf("create dir for %s: %w", relPath, mkErr)
				return
			}
			if extractErr := extractSingleFileFromContainer(ctx, dc, ctr.GetContainerID(), srcPath, dstPath); extractErr != nil {
				fmt.Printf("db2test: skipping instance file %s (non-fatal): %v\n", relPath, extractErr)
			} else {
				fmt.Printf("db2test: extracted instance file %s\n", dstPath)
			}
		}

		// db2nodes.cfg extracted from the DB2 container contains the DB2
		// container's hostname. libdb2.so.1 reads this file and checks that
		// the current machine's hostname matches node 0. If they differ, the
		// library returns SQL1042C on every connection attempt — even remote
		// TCPIP connections. Overwrite with the test runner's own hostname.
		nodesCfgPath := filepath.Join(instanceSqllib, "db2nodes.cfg")
		if localHost, hostnameErr := os.Hostname(); hostnameErr == nil {
			nodesCfg := fmt.Sprintf("0 %s 0\n", localHost)
			if writeErr := os.WriteFile(nodesCfgPath, []byte(nodesCfg), 0o644); writeErr == nil {
				fmt.Printf("db2test: rewrote db2nodes.cfg with local hostname %q\n", localHost)
			}
		}

		// Extract instance-level message catalogs — libdb2.so.1 checks
		// $INSTHOME/sqllib/msg/ before $DB2DIR/msg/.
		instanceMsgSrc := instanceSqllib + "/msg"
		instanceMsgDst := filepath.Join(instanceSqllib, "msg")
		if instanceMsgTS, _, instanceMsgErr := dc.CopyFromContainer(ctx, ctr.GetContainerID(), instanceMsgSrc); instanceMsgErr == nil {
			defer instanceMsgTS.Close()
			instanceMsgTR := tar.NewReader(instanceMsgTS)
			for {
				hdr, err := instanceMsgTR.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					break
				}
				relName := strings.TrimPrefix(strings.TrimPrefix(hdr.Name, "msg"), "/")
				if relName == "" {
					continue
				}
				dstPath := filepath.Join(instanceMsgDst, relName)
				switch hdr.Typeflag {
				case tar.TypeDir:
					_ = os.MkdirAll(dstPath, 0o755)
				case tar.TypeReg:
					_ = os.MkdirAll(filepath.Dir(dstPath), 0o755)
					if f, createErr := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)|0o644); createErr == nil {
						_, _ = io.Copy(f, instanceMsgTR)
						f.Close()
					}
				case tar.TypeSymlink:
					_ = os.Symlink(filepath.Base(hdr.Linkname), dstPath)
				}
			}
			fmt.Printf("db2test: extracted instance msg to %s\n", instanceMsgDst)
		} else {
			fmt.Printf("db2test: instance msg not available (non-fatal): %v\n", instanceMsgErr)
		}

		// Ensure the instance msg directory has locale subdirs pointing to the
		// global $DB2DIR/msg locale dirs. In the container the instance sqllib/msg/
		// entries are symlinks → $DB2DIR/msg, so the tar extraction skips them,
		// leaving the directory empty. DB2 CLI checks the instance path first and
		// fails with SQL10007N if the locale dir is absent.
		{
			globalMsgBase := filepath.Join(db2BaseDir, "msg")
			if err := os.MkdirAll(instanceMsgDst, 0o755); err == nil {
				if entries, readErr := os.ReadDir(globalMsgBase); readErr == nil {
					for _, e := range entries {
						linkPath := filepath.Join(instanceMsgDst, e.Name())
						if _, statErr := os.Lstat(linkPath); os.IsNotExist(statErr) {
							_ = os.Symlink(filepath.Join(globalMsgBase, e.Name()), linkPath)
						}
					}
				}
			}
		}

		// Create instance sqllib/security64 directory structure with IBM symlink.
		// In the container, sqllib/security64/plugin/IBM -> $DB2DIR/security64/plugin/IBM.
		// Recreating this allows DB2 CLI to find authentication plugins.
		{
			instanceSec64Plugin := filepath.Join(instanceSqllib, "security64", "plugin")
			for _, dir := range []string{"client", "group", "server"} {
				_ = os.MkdirAll(filepath.Join(instanceSec64Plugin, dir), 0o755)
			}
			ibmLink := filepath.Join(instanceSec64Plugin, "IBM")
			if _, statErr := os.Lstat(ibmLink); os.IsNotExist(statErr) {
				_ = os.Symlink(filepath.Join(db2BaseDir, "security64", "plugin", "IBM"), ibmLink)
			}
		}

		// Diagnostic: print which critical paths exist vs. are missing.
		for _, checkPath := range []string{
			"/database/config/db2inst1/sqllib/db2systm",
			"/database/config/db2inst1/sqllib/db2nodes.cfg",
			"/database/config/db2inst1/sqllib/cfg/db2cli.ini",
			"/opt/ibm/db2/V12.1/msg/en_US.iso88591",
			"/database/config/db2inst1/sqllib/msg/en_US.iso88591",
			"/opt/ibm/db2/V12.1/security64/plugin/IBM/client/IBMOSauthclient.so",
			"/database/config/db2inst1/sqllib/security64/plugin/IBM",
		} {
			if _, statErr := os.Stat(checkPath); statErr == nil {
				fmt.Printf("db2test: EXISTS: %s\n", checkPath)
			} else {
				fmt.Printf("db2test: MISSING: %s\n", checkPath)
			}
		}

		// Register db2LibInstallDir with ldconfig so dlopen finds libdb2.so.1 by
		// name. db2LibInstallDir is not in ldconfig's default search path, so we
		// drop a config file into /etc/ld.so.conf.d/ before running ldconfig.
		if err := os.MkdirAll("/etc/ld.so.conf.d", 0o755); err != nil {
			db2LibErr = fmt.Errorf("create /etc/ld.so.conf.d: %w", err)
			return
		}
		if err := os.WriteFile(db2LdConfFile, []byte(db2LibInstallDir+"\n"), 0o644); err != nil {
			db2LibErr = fmt.Errorf("write %s: %w", db2LdConfFile, err)
			return
		}
		// Verify the primary library file was extracted before attempting to load.
		libPath := filepath.Join(db2LibInstallDir, "libdb2.so.1")
		if _, statErr := os.Stat(libPath); statErr != nil {
			db2LibErr = fmt.Errorf("libdb2.so.1 not found at %s after extraction: %w", libPath, statErr)
			return
		}
		fmt.Printf("db2test: extracted %s OK, loading...\n", libPath)

		// Run ldconfig so the system linker cache includes db2LibInstallDir.
		// This is best-effort; the full-path dlopen below is the authoritative load.
		if out, ldErr := exec.Command("ldconfig").CombinedOutput(); ldErr != nil {
			fmt.Printf("db2test: ldconfig warning: %v\n%s\n", ldErr, out)
		}

		// Print runtime environment so we can verify DB2DIR is set and the
		// msg directory actually has the expected contents on disk.
		fmt.Printf("db2test: DB2DIR=%q LANG=%q\n", os.Getenv("DB2DIR"), os.Getenv("LANG"))
		if entries, lsErr := os.ReadDir(filepath.Join(db2BaseDir, "msg")); lsErr == nil {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			fmt.Printf("db2test: msg/ entries: %v\n", names)
		} else {
			fmt.Printf("db2test: msg/ read error: %v\n", lsErr)
		}
		if entries, lsErr := os.ReadDir(filepath.Join(db2BaseDir, "msg", "en_US.iso88591")); lsErr == nil {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			fmt.Printf("db2test: msg/en_US.iso88591/ (%d files): %v\n", len(names), names[:min(10, len(names))])
		} else {
			fmt.Printf("db2test: msg/en_US.iso88591/ error: %v\n", lsErr)
		}

		// Use the full absolute path so dlopen resolves libdb2.so.1 without
		// depending on the linker cache.
		db2LibErr = db2cli.LoadLibraryFromPath(libPath)
		if db2LibErr != nil {
			return
		}

		// Absorb the first-call SQLAllocHandle(ENV) failure. The DB2 CLI library
		// runs one-time global initialization on the very first SQLAllocHandle call;
		// that init returns SQL_ERROR even though it partially succeeds, causing all
		// subsequent calls to work normally. Calling SQLAllocHandle once here
		// consumes that failure so that real connection attempts all succeed.
		var warmupHenv db2cli.SQLHENV
		if r := db2cli.SQLAllocHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(db2cli.SQL_NULL_HENV), (*db2cli.SQLHANDLE)(&warmupHenv)); r == db2cli.SQL_SUCCESS || r == db2cli.SQL_SUCCESS_WITH_INFO {
			db2cli.SQLFreeHandle(db2cli.SQL_HANDLE_ENV, db2cli.SQLHANDLE(warmupHenv))
		}
		fmt.Printf("db2test: DB2 CLI library warm-up complete\n")
	})
	return db2LibErr
}

// extractSingleFileFromContainer copies one regular file from a running container
// to dstPath. Non-fatal callers should just log the returned error.
func extractSingleFileFromContainer(ctx context.Context, dc *dockerclient.Client, containerID, srcPath, dstPath string) error {
	ts, _, err := dc.CopyFromContainer(ctx, containerID, srcPath)
	if err != nil {
		return err
	}
	defer ts.Close()
	tr := tar.NewReader(ts)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("file not found in tar for %s", srcPath)
		}
		if err != nil {
			return fmt.Errorf("reading tar: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)|0o644)
		if err != nil {
			return fmt.Errorf("create %s: %w", dstPath, err)
		}
		_, copyErr := io.Copy(f, tr)
		f.Close()
		return copyErr
	}
}

// containerDirectDSN returns a DSN that connects directly to the DB2 container
// via its bridge network IP and port 50000, bypassing the host.docker.internal
// NAT route.  Returns "" if the IP cannot be determined.
func containerDirectDSN(ctx context.Context, ctr testcontainers.Container) string {
	dc, err := dockerclient.NewClientWithOpts(dockerclient.FromEnv, dockerclient.WithAPIVersionNegotiation())
	if err != nil {
		return ""
	}
	defer dc.Close()
	info, err := dc.ContainerInspect(ctx, ctr.GetContainerID())
	if err != nil {
		return ""
	}
	for _, nw := range info.NetworkSettings.Networks {
		if nw.IPAddress != "" {
			dsn := fmt.Sprintf(
				"DATABASE=%s;HOSTNAME=%s;PORT=50000;PROTOCOL=TCPIP;UID=%s;PWD=%s",
				db2Database, nw.IPAddress, db2User, db2Password,
			)
			fmt.Printf("db2test: using direct container IP DSN: %s:50000\n", nw.IPAddress)
			return dsn
		}
	}
	return ""
}

// SetupTest returns a TestDB connected to the shared DB2 container for this
// test run.  The container is started once per process (via AcquireSharedContainer
// called from TestMain) and torn down after all tests finish.
//
// Each call opens its own *sql.DB so connection-pool state is isolated between
// tests.  The returned connection is closed in t.Cleanup.
//
// On macOS the test uses IBM's clidriver (libdb2.dylib) — a TCP-only client that
// needs no SysV IPC. Set DB2_DYLIB_PATH to the dylib path, or install ibm_db via
// pip3 (pip3 install ibm_db bundles the clidriver).  The test is skipped when the
// shared container is unavailable (Docker not running, no icr.io credentials, etc.).
// SetupTest acquires the shared DB2 container (starting it if needed), opens a
// new *sql.DB connection, and returns a TestDB ready for use. If the shared
// container is unavailable (e.g. Docker is not running or the container failed
// to start) the test is skipped rather than failed, so the test suite does not
// block CI when DB2 integration tests are not configured. The connection is
// closed automatically via t.Cleanup.
func SetupTest(t *testing.T) *TestDB {
	t.Helper()

	ctx := t.Context()
	_, dsn, err := AcquireSharedContainer(ctx)
	if err != nil {
		t.Skipf("skipping DB2 integration test: shared container unavailable: %v", err)
	}

	t.Logf("db2test: connecting to DSN=%s", dsn)

	var db *sql.DB
	var lastPingErr error
	require.Eventually(t, func() bool {
		if db != nil {
			_ = db.Close()
		}
		db, err = sql.Open("db2-cli", dsn)
		if err != nil {
			lastPingErr = fmt.Errorf("sql.Open: %w", err)
			t.Logf("db2test: sql.Open failed: %v", lastPingErr)
			return false
		}
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(5)
		db.SetConnMaxLifetime(5 * time.Minute)
		lastPingErr = db.PingContext(ctx)
		if lastPingErr != nil {
			t.Logf("db2test: ping failed: %v", lastPingErr)
		}
		return lastPingErr == nil
	}, 2*time.Minute, 3*time.Second, "DB2 did not become reachable")

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	return &TestDB{DB: db, T: t, DSN: dsn}
}

// EnableASNCDC starts the DB2 SQL Replication capture daemon and registers the
// given tables for CDC.  Must be called after tables already exist.
//
// Setup sequence mirrors IBM's ASNCDC SQL replication setup:
//  1. Start the capture daemon (retry until ready).
//  2. Call ADDTABLE for each table.
//  3. Set STATE='A' in IBMSNAP_REGISTER for each table.
//  4. Reinit the capture daemon so it picks up the new registrations.
//  5. Wait for SYNCHPOINT to become non-null (daemon has processed the table).
//
// When DB2_USE_LOCAL=1 (test binary running inside the DB2 container), the
// ASNCDC.ASNCDCSERVICES and ASNCDC.ADDTABLE SQL UDFs are not available in
// DB2 Community Edition, so this method uses os/exec shell commands instead.
func (db *TestDB) EnableASNCDC(schema string, tables []string) {
	db.T.Helper()

	// DB2 Community Edition does not ship the ASNCDC.ASNCDCSERVICES / ADDTABLE
	// UDFs (they are part of the IBM ASNTOOLS package).  Use the local-mode path
	// (direct shell commands via docker exec on macOS, direct su on Linux) for
	// both DB2_USE_LOCAL=1 and macOS native mode.
	if os.Getenv("DB2_USE_LOCAL") == "1" || runtime.GOOS == "darwin" {
		db.enableASNCDCLocal(schema, tables)
		return
	}

	ctx := db.T.Context()

	// Start the capture daemon — retry because the first call after the DB
	// restarts with LOGARCHMETH1=LOGRETAIN may fail transiently.
	require.Eventually(db.T, func() bool {
		_, err := db.ExecContext(ctx, "VALUES ASNCDC.ASNCDCSERVICES('start','asncdc')")
		return err == nil
	}, 2*time.Minute, 5*time.Second, "ASNCDC service did not start")

	for _, table := range tables {
		_, err := db.ExecContext(ctx,
			fmt.Sprintf("VALUES ASNCDC.ADDTABLE('%s','%s')", schema, table),
		)
		require.NoError(db.T, err, "ASNCDC.ADDTABLE(%s, %s)", schema, table)

		// Activate the registration — ADDTABLE creates it with STATE='I' (inactive).
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			"UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'A' WHERE SOURCE_OWNER = '%s' AND SOURCE_TABLE = '%s'",
			schema, table,
		))
		require.NoError(db.T, err, "activate IBMSNAP_REGISTER %s.%s", schema, table)
	}

	// Reinit the capture daemon so it picks up newly registered tables.
	_, err := db.ExecContext(ctx, "VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc')")
	require.NoError(db.T, err, "ASNCDC reinit")

	// Wait for the daemon to process at least one table registration and populate SYNCHPOINT.
	require.Eventually(db.T, func() bool {
		var count int
		_ = db.QueryRowContext(ctx,
			fmt.Sprintf(
				"SELECT COUNT(*) FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER = '%s' AND SYNCHPOINT IS NOT NULL",
				schema,
			),
		).Scan(&count)
		return count > 0
	}, 2*time.Minute, 3*time.Second, "SYNCHPOINT did not become non-null after ASNCDC reinit")
}

// enableASNCDCLocal is the local-mode implementation of EnableASNCDC used when
// DB2_USE_LOCAL=1.  It replaces the ASNCDC.ASNCDCSERVICES and ASNCDC.ADDTABLE
// SQL UDFs (which require the IBM ASNTOOLS package not present in DB2 Community
// Edition) with direct shell commands and SQL DDL.
func (db *TestDB) enableASNCDCLocal(schema string, tables []string) {
	db.T.Helper()
	ctx := db.T.Context()

	// Set up all table registrations BEFORE starting asncap. If asncap starts
	// and finds a registered table without DATA CAPTURE CHANGES it stops
	// immediately with ASN0009E. addTableLocal sets DATA CAPTURE CHANGES as
	// part of registration, so all tables must be ready before asncap starts.
	for _, table := range tables {
		if err := addTableLocal(ctx, db.DB, schema, table); err != nil {
			db.T.Fatalf("enableASNCDCLocal: addTableLocal(%s.%s): %v", schema, table, err)
		}
		// addTableLocal now inserts STATE='A' directly with null synchpoints,
		// matching IBM's ADDTABLE procedure — no separate UPDATE needed.
	}

	// Ensure asncap is running after all tables have DATA CAPTURE CHANGES set.
	// All tables must be registered before starting asncap — if asncap encounters
	// a registered table without DATA CAPTURE CHANGES it stops immediately.
	if err := startAsncapLocal(); err != nil {
		db.T.Fatalf("enableASNCDCLocal: startAsncapLocal: %v", err)
	}

	// Signal asncap to re-read the registration table and start capturing the
	// newly registered tables.
	reinitOut, reinitErr := runAsDB2Inst1(fmt.Sprintf(
		"asnccmd capture_server=%s capture_schema=ASNCDC reinit",
		db2Database,
	))
	fmt.Printf("db2test: asnccmd reinit output: %s\n", reinitOut)
	if reinitErr != nil {
		db.T.Logf("asnccmd reinit warning (non-fatal): %v", reinitErr)
	}

	// IBM recommends waiting ~15 seconds after reinit before polling SYNCHPOINT.
	// This gives asncap time to process the REINIT, pick up the new registrations,
	// and set SYNCHPOINT in IBMSNAP_REGISTER for the new tables.
	time.Sleep(15 * time.Second)

	// Verify asncap is still running after reinit.
	checkPgrepLocal := func() string {
		if darwinContainerName != "" {
			out, _ := exec.Command("docker", "exec", darwinContainerName, "pgrep", "-f", "capture_server="+db2Database).Output()
			return strings.TrimSpace(string(out))
		}
		out, _ := exec.Command("pgrep", "-f", "capture_server="+db2Database).Output()
		return strings.TrimSpace(string(out))
	}
	if checkPgrepLocal() == "" {
		db.T.Logf("db2test: WARNING: asncap is not running after reinit — attempting restart")
		if err := startAsncapLocal(); err != nil {
			db.T.Logf("db2test: WARNING: asncap restart failed: %v", err)
		}
		time.Sleep(5 * time.Second)
	}

	// Poll until asncap has set SYNCHPOINT for each newly registered table,
	// or up to 30s. This ensures the snapshot connector gets a valid starting
	// position from captureCurrentCSN rather than CSN(0).
	for _, table := range tables {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var sp []byte
			if err := db.DB.QueryRowContext(ctx,
				"SELECT SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=? AND SOURCE_TABLE=? AND SYNCHPOINT IS NOT NULL FETCH FIRST 1 ROW ONLY",
				schema, table,
			).Scan(&sp); err == nil && len(sp) > 0 {
				fmt.Printf("db2test: asncap set SYNCHPOINT for %s.%s: %X\n", schema, table, sp)
				break
			}
			time.Sleep(3 * time.Second)
		}
	}

	// Print the asncap log tail to help diagnose capture issues.
	var logOut []byte
	if darwinContainerName != "" {
		logOut, _ = exec.Command("docker", "exec", darwinContainerName, "cat", "/tmp/asncap.log").Output()
	} else {
		logOut, _ = os.ReadFile("/tmp/asncap.log")
	}
	if len(logOut) > 0 {
		tail := logOut
		if len(tail) > 3000 {
			tail = tail[len(tail)-3000:]
		}
		fmt.Printf("db2test: asncap log (last 3000 bytes):\n%s\n", tail)
	}
}

// runAsDB2Inst1 executes cmdStr as a shell command under the db2inst1 user.
// The '-' flag loads the full login environment (DB2INSTANCE, DB2DIR, PATH, etc.)
// so that db2, asncap, asnccmd, and other DB2 tools are available.
//
// On macOS (darwin), when DB2_DARWIN_CONTAINER is set, commands are dispatched
// via "docker exec {container} su - db2inst1 -c {cmd}" because the DB2 binaries
// only exist inside the container, not on the macOS host.
func runAsDB2Inst1(cmdStr string) ([]byte, error) {
	if darwinContainerName != "" {
		cmd := exec.Command("docker", "exec", darwinContainerName, "su", "-", "db2inst1", "-c", cmdStr)
		return cmd.CombinedOutput()
	}
	cmd := exec.Command("su", "-", "db2inst1", "-c", cmdStr)
	return cmd.CombinedOutput()
}

// findAsnScript searches the DB2 installation directory for a named SQL script
// and returns the first match, or empty string if not found.
//
// On macOS, the search runs inside the Docker container via docker exec.
func findAsnScript(name string) string {
	var out []byte
	var err error
	if darwinContainerName != "" {
		out, err = exec.Command("docker", "exec", darwinContainerName, "find", "/opt/ibm/db2", "-name", name, "-type", "f").Output()
	} else {
		out, err = exec.Command("find", "/opt/ibm/db2", "-name", name, "-type", "f").Output()
	}
	if err != nil || len(out) == 0 {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) > 0 && lines[0] != "" {
		return lines[0]
	}
	return ""
}

// createASNCDCTablesIfNeeded creates the ASNCDC SQL Replication control tables
// required by asncap if they do not already exist.  It locates IBM's asnctlw.sql
// script, rewrites all ASN schema references to ASNCDC, and runs it via db2inst1.
// After the base script it also applies any migration scripts (asncap*fp.sql) that
// add columns or tables introduced in later DB2 fix-packs.
func createASNCDCTablesIfNeeded(ctx context.Context, db *sql.DB) error {
	// Check whether the ASNCDC schema already exists.
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = 'ASNCDC'",
	).Scan(&n); err != nil {
		return fmt.Errorf("checking ASNCDC schema: %w", err)
	}
	if n > 0 {
		fmt.Printf("db2test: ASNCDC schema already exists, checking CAPPARMS...\n")
		// Apply any missing columns from newer DB2 fix-packs even on existing schemas.
		if err := ensureMissingColumns(ctx, db); err != nil {
			fmt.Printf("db2test: ensureMissingColumns warning: %v\n", err)
		}
		return ensureCAPPARMSRow(ctx, db)
	}

	// Find the base control-table creation script.
	scriptPath := findAsnScript("asnctlw.sql")
	if scriptPath == "" {
		return errors.New("asnctlw.sql not found in DB2 installation (required to set up ASNCDC)")
	}
	fmt.Printf("db2test: found asnctlw.sql at %s\n", scriptPath)

	var content []byte
	var err error
	if darwinContainerName != "" {
		// scriptPath lives inside the container; read it via docker exec cat.
		content, err = exec.Command("docker", "exec", darwinContainerName, "cat", scriptPath).Output()
		if err != nil {
			return fmt.Errorf("reading %s from container %s: %w", scriptPath, darwinContainerName, err)
		}
	} else {
		content, err = os.ReadFile(scriptPath)
		if err != nil {
			return fmt.Errorf("reading %s: %w", scriptPath, err)
		}
	}

	// Rewrite all ASN schema references to ASNCDC, then strip multi-partition
	// tablespace clauses so the script runs on single-partition DB2 12.1.
	modified := rewriteASNSchemaInSQL(string(content), "ASN", "ASNCDC")
	modified = stripTablespaceForSinglePartition(modified)

	tmp, err := os.CreateTemp("", "asncdc-*.sql")
	if err != nil {
		return fmt.Errorf("creating temp SQL file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.WriteString(modified); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temp SQL file: %w", err)
	}
	tmp.Close()
	_ = os.Chmod(tmpPath, 0o644) // readable by db2inst1

	// asnctlw.sql uses ; as the statement terminator in all DB2 versions.
	// Do NOT auto-detect: the file contains ! in comments (e.g. "...tables!!!")
	// which would cause false-positive terminator detection.
	//
	// Use -t (built-in semicolon mode) to avoid passing -td; directly, because
	// bash treats ; as a command separator inside su -c "..." strings in some
	// DB2 container environments, producing DB21002E.
	//
	// On macOS, the temp file lives on the host; docker cp it into the container
	// so that "db2 -f /path" resolves correctly inside the container.
	sqlRunPath := tmpPath
	if darwinContainerName != "" {
		// Use a /tmp path in the container (the macOS host temp path like
		// /var/folders/... does not exist inside the container).
		ctrSQLPath := "/tmp/" + filepath.Base(tmpPath)
		if cpErr := exec.Command("docker", "cp", tmpPath, darwinContainerName+":"+ctrSQLPath).Run(); cpErr != nil {
			fmt.Printf("db2test: docker cp asnctlw.sql warning: %v\n", cpErr)
		} else {
			sqlRunPath = ctrSQLPath
			defer exec.Command("docker", "exec", darwinContainerName, "rm", "-f", ctrSQLPath).Run() //nolint:errcheck
		}
	}
	out, _ := runAsDB2Inst1(fmt.Sprintf(
		"db2 connect to %s && db2 -t -f '%s'",
		db2Database, sqlRunPath,
	))
	fmt.Printf("db2test: asnctlw.sql output:\n%s\n", out)

	// Apply fix-pack migration scripts to add columns / tables introduced in
	// DB2 11.4+ (WARNTXSZ, WARNLOGAPI, STALE, etc. in CAPPARMS).
	// The script uses ';' as the statement terminator.
	// It also uses '!capschema!' and '!captablespace!' as template placeholders
	// that must be substituted before running.
	for _, fpScript := range []string{
		findAsnScript("asncapluwv1140fp.sql"),
	} {
		if fpScript == "" {
			continue
		}
		var fpContent []byte
		var readErr error
		if darwinContainerName != "" {
			// fpScript lives inside the container; read it via docker exec cat.
			fpContent, readErr = exec.Command("docker", "exec", darwinContainerName, "cat", fpScript).Output()
		} else {
			fpContent, readErr = os.ReadFile(fpScript)
		}
		if readErr != nil {
			fmt.Printf("db2test: skipping %s (read error: %v)\n", fpScript, readErr)
			continue
		}
		fpModified := rewriteASNSchemaInSQL(string(fpContent), "ASN", "ASNCDC")
		// Replace template placeholders — the script ships with !capschema! and
		// !captablespace! tokens (both upper and lower case) that must be swapped
		// before execution.
		fpModified = strings.ReplaceAll(fpModified, "!CAPSCHEMA!", "ASNCDC")
		fpModified = strings.ReplaceAll(fpModified, "!capschema!", "ASNCDC")
		fpModified = strings.ReplaceAll(fpModified, "!CAPTABLESPACE!", "USERSPACE1")
		fpModified = strings.ReplaceAll(fpModified, "!captablespace!", "USERSPACE1")
		fpTmp, tmpErr := os.CreateTemp("", "asncdc-fp-*.sql")
		if tmpErr != nil {
			continue
		}
		fpTmpPath := fpTmp.Name()
		_, _ = fpTmp.WriteString(fpModified)
		fpTmp.Close()
		_ = os.Chmod(fpTmpPath, 0o644)
		defer os.Remove(fpTmpPath)

		fpRunPath := fpTmpPath
		if darwinContainerName != "" {
			ctrFPPath := "/tmp/" + filepath.Base(fpTmpPath)
			if cpErr := exec.Command("docker", "cp", fpTmpPath, darwinContainerName+":"+ctrFPPath).Run(); cpErr != nil {
				fmt.Printf("db2test: docker cp fp script warning: %v\n", cpErr)
			} else {
				fpRunPath = ctrFPPath
				defer exec.Command("docker", "exec", darwinContainerName, "rm", "-f", ctrFPPath).Run() //nolint:errcheck
			}
		}
		fpOut, _ := runAsDB2Inst1(fmt.Sprintf(
			"db2 connect to %s && db2 -t -f '%s'",
			db2Database, fpRunPath,
		))
		fmt.Printf("db2test: %s output:\n%s\n", filepath.Base(fpScript), fpOut)
	}

	if err := ensureMissingColumns(ctx, db); err != nil {
		fmt.Printf("db2test: ensureMissingColumns warning: %v\n", err)
	}

	// Ensure IBMQREP_CAPCMD exists so asnccmd can send reinit signals.
	if _, createErr := db.ExecContext(ctx, `
		CREATE TABLE ASNCDC.IBMQREP_CAPCMD (
			QMGR       VARCHAR(48),
			RECVQ      VARCHAR(48),
			IGNERR     CHAR(1)       NOT NULL WITH DEFAULT 'N',
			COMMAND    VARCHAR(2048) NOT NULL,
			STATUS     CHAR(1)                WITH DEFAULT 'P',
			CAPSCHEMA  VARCHAR(128)  NOT NULL,
			CMD_TIME   TIMESTAMP     NOT NULL WITH DEFAULT CURRENT TIMESTAMP
		)
	`); createErr != nil && !isAlreadyExistsErr(createErr) {
		fmt.Printf("db2test: IBMQREP_CAPCMD creation warning: %v\n", createErr)
	}

	return ensureCAPPARMSRow(ctx, db)
}

// ensureMissingColumns adds columns that are present in newer DB2 fix-packs but
// absent from the 11.4.0 init scripts we execute.  Safe to call on every
// startup: isAlreadyExistsErr suppresses SQL0612N duplicate-column errors.
func ensureMissingColumns(ctx context.Context, db *sql.DB) error {
	// IBMSNAP_CAPPARMS columns added in 11.4.0 fix-packs.
	for _, stmt := range []string{
		"ALTER TABLE ASNCDC.IBMSNAP_CAPPARMS ADD COLUMN STALE INTEGER NOT NULL WITH DEFAULT 3600",
		"ALTER TABLE ASNCDC.IBMSNAP_CAPPARMS ADD COLUMN WARNTXSZ INTEGER NOT NULL WITH DEFAULT 0",
		"ALTER TABLE ASNCDC.IBMSNAP_CAPPARMS ADD COLUMN WARNLOGAPI INTEGER NOT NULL WITH DEFAULT 0",
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil && !isAlreadyExistsErr(err) {
			fmt.Printf("db2test: CAPPARMS column warning: %v\n", err)
		}
	}
	// IBMQREP_COLVERSION.COORDINATETYPE added in DB2 12.1 Q Capture migration.
	// asncap 12.1.4.0 crashes with SQL0206N if this column is absent.
	if _, err := db.ExecContext(ctx,
		"ALTER TABLE ASNCDC.IBMQREP_COLVERSION ADD COLUMN COORDINATETYPE VARCHAR(8) WITH DEFAULT NULL",
	); err != nil && !isAlreadyExistsErr(err) {
		fmt.Printf("db2test: IBMQREP_COLVERSION COORDINATETYPE warning: %v\n", err)
	}
	return nil
}

// rewriteASNSchemaInSQL replaces all occurrences of fromSchema (used as a SQL
// identifier) with toSchema in a DB2 SQL script.  The replacement is
// word-boundary–aware to avoid partial matches like "ORASN.TABLE".
func rewriteASNSchemaInSQL(script, fromSchema, toSchema string) string {
	q := regexp.QuoteMeta(fromSchema)
	// Qualified names: "ASN.TABLE" → "ASNCDC.TABLE"
	result := regexp.MustCompile(`\b`+q+`\.`).ReplaceAllString(script, toSchema+".")
	// CREATE/USE SCHEMA declarations: "SCHEMA ASN" → "SCHEMA ASNCDC"
	result = regexp.MustCompile(`\bSCHEMA\s+`+q+`\b`).ReplaceAllString(result, "SCHEMA "+toSchema)
	return result
}

// stripTablespaceForSinglePartition removes multi-partition tablespace clauses from
// IBM's asnctlw.sql so it works on single-partition DB2 12.1 containers.
// The script was written for DB2 9.x/10.x with multi-partition syntax.
// We strip:
//   - Entire "CREATE TABLESPACE … ;" blocks (we don't need separate tablespaces)
//   - "IN TSASNCA;", "IN TSASNUOW;", "IN TSASNAA;" lines (table placement clauses)
//   - "IN TSASNCA" without semicolon (inline placement clauses)
//
// Tables land in the default tablespace, which is fine for tests.
func stripTablespaceForSinglePartition(sql string) string {
	lines := strings.Split(sql, "\n")
	var out []string
	skipUntilSemicolon := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Start of a CREATE TABLESPACE block — skip until we see the closing ");".
		if strings.HasPrefix(trimmed, "CREATE") && strings.Contains(trimmed, "TABLESPACE") {
			skipUntilSemicolon = true
		}
		if skipUntilSemicolon {
			if strings.HasSuffix(trimmed, ");") || trimmed == ");" {
				skipUntilSemicolon = false
			}
			continue
		}

		// Strip "IN TSASN*" table-placement lines.  When the line ends with ';'
		// (the statement terminator), emit just ';' so the CREATE TABLE statement
		// stays properly terminated.  Without this, removing "IN TSASNCA;" leaves
		// the enclosing CREATE TABLE without a terminator and db2 runs statements together.
		if regexp.MustCompile(`(?i)^\s*IN\s+TSASN\w*\s*;?\s*$`).MatchString(line) {
			if strings.Contains(line, ";") {
				out = append(out, ";")
			}
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// ensureCAPPARMSRow inserts a CAPPARMS row if the table is empty.
// asncap will not start unless IBMSNAP_CAPPARMS has exactly one row.
func ensureCAPPARMSRow(ctx context.Context, db *sql.DB) error {
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM ASNCDC.IBMSNAP_CAPPARMS",
	).Scan(&n); err != nil {
		return fmt.Errorf("checking CAPPARMS: %w", err)
	}
	if n > 0 {
		return nil
	}

	// Preferred: copy the IBM-supplied defaults from the ASN schema.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO ASNCDC.IBMSNAP_CAPPARMS
		SELECT * FROM ASN.IBMSNAP_CAPPARMS FETCH FIRST 1 ROW ONLY
	`); err == nil {
		fmt.Printf("db2test: CAPPARMS row copied from ASN schema\n")
		return nil
	}

	// Fallback 1: all-defaults row (works when every column has a DEFAULT).
	if _, err := db.ExecContext(ctx, "INSERT INTO ASNCDC.IBMSNAP_CAPPARMS DEFAULT VALUES"); err == nil {
		fmt.Printf("db2test: CAPPARMS DEFAULT VALUES row inserted\n")
		return nil
	}

	// Fallback 2: explicit minimal row with the columns we know about.
	if _, err := db.ExecContext(ctx,
		"INSERT INTO ASNCDC.IBMSNAP_CAPPARMS (STARTMODE, ARCH_LEVEL) VALUES ('WARMSI', '1018')",
	); err != nil {
		return fmt.Errorf("inserting minimal CAPPARMS row: %w", err)
	}
	fmt.Printf("db2test: CAPPARMS minimal row inserted\n")
	return nil
}

// isAlreadyExistsErr returns true when a DB2 error indicates the object already
// exists (SQL0601N) or a unique index is already defined (SQL0603N).
func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	// SQL0601N — object already exists (table, index, etc.)
	// SQL0603N — unique index already defined
	// SQL0612N — duplicate column name (ADD COLUMN for already-existing column)
	return strings.Contains(s, "SQL0601N") || strings.Contains(s, "already exists") ||
		strings.Contains(s, "SQL0603N") || strings.Contains(s, "SQL0612N")
}

// startAsncapLocal starts the asncap SQL Replication capture daemon if it is
// not already running.  asncap must run as the db2inst1 instance owner.
//
// On macOS (darwin), process checks and log reads are routed through docker exec
// because asncap runs inside the DB2 container, not on the macOS host.
//
// Before starting, any active (STATE='A') registrations for tables that no
// longer have the DATA CAPTURE CHANGES attribute are deactivated to STATE='I'.
// asncap stops immediately with ASN0009E if it encounters such a registration,
// which happens when a source table was dropped-and-recreated between runs in
// a persistent test container.
func startAsncapLocal() error {
	// Deactivate stale registrations that lack DATA CAPTURE CHANGES.
	// These would cause asncap to crash on startup with ASN0009E.
	// We use a best-effort DB2 shell call; errors are non-fatal.
	deactivateSQL := "UPDATE ASNCDC.IBMSNAP_REGISTER SET STATE = 'I' WHERE STATE = 'A' AND NOT EXISTS (SELECT 1 FROM SYSCAT.TABLES T WHERE T.TABSCHEMA = ASNCDC.IBMSNAP_REGISTER.SOURCE_OWNER AND T.TABNAME = ASNCDC.IBMSNAP_REGISTER.SOURCE_TABLE AND T.DATACAPTURE <> 'N')"
	deactivateScript := fmt.Sprintf(`db2 connect to %s && db2 "%s"`, db2Database, deactivateSQL)
	if out, err := runAsDB2Inst1(deactivateScript); err != nil {
		fmt.Printf("db2test: deactivate stale registrations warning (non-fatal): %v\n%s\n", err, out)
	}

	// checkPgrep returns the pids of asncap processes for TESTDB, empty if none.
	checkPgrep := func() string {
		if darwinContainerName != "" {
			out, _ := exec.Command("docker", "exec", darwinContainerName, "pgrep", "-f", "capture_server="+db2Database).Output()
			return strings.TrimSpace(string(out))
		}
		out, _ := exec.Command("pgrep", "-f", "capture_server="+db2Database).Output()
		return strings.TrimSpace(string(out))
	}
	// readAsncapLog returns the contents of /tmp/asncap.log (in container or host).
	readAsncapLog := func() []byte {
		if darwinContainerName != "" {
			out, _ := exec.Command("docker", "exec", darwinContainerName, "cat", "/tmp/asncap.log").Output()
			return out
		}
		out, _ := os.ReadFile("/tmp/asncap.log")
		return out
	}

	// Check if a capture process for TESTDB is already running.
	if checkPgrep() != "" {
		fmt.Printf("db2test: asncap already running (pid: %s)\n", checkPgrep())
		return nil
	}

	fmt.Printf("db2test: starting asncap capture daemon for %s/ASNCDC...\n", db2Database)
	out, err := runAsDB2Inst1(fmt.Sprintf(
		"nohup asncap capture_server=%s capture_schema=ASNCDC > /tmp/asncap.log 2>&1 &",
		db2Database,
	))
	if err != nil {
		return fmt.Errorf("launching asncap: %v: %s", err, out)
	}

	// Wait up to 30 s for the process to appear, then wait for ASN0100I
	// (successfully initialized) before returning.  Returning as soon as the
	// PID appears is too early: asncap needs another ~2 s to read the DB2 log
	// and finish init.  If we send "asnccmd reinit" during that window the
	// signal arrives before asncap is ready and is silently dropped.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if pids := checkPgrep(); pids != "" {
			fmt.Printf("db2test: asncap started (pid: %s)\n", pids)
			// Wait for the "initialized successfully" log entry before returning
			// so that a subsequent asnccmd reinit is processed correctly.
			initDeadline := time.Now().Add(30 * time.Second)
			for time.Now().Before(initDeadline) {
				if logOut := readAsncapLog(); len(logOut) > 0 &&
					(strings.Contains(string(logOut), "ASN0100I") || strings.Contains(string(logOut), "ASN0109I")) {
					fmt.Printf("db2test: asncap fully initialized\n")
					return nil
				}
				time.Sleep(1 * time.Second)
			}
			fmt.Printf("db2test: WARNING: asncap started but initialization log not seen within 30s\n")
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	// Log the asncap output to help diagnose startup failures.
	if logOut := readAsncapLog(); len(logOut) > 0 {
		fmt.Printf("db2test: asncap startup log:\n%s\n", logOut)
	}
	return errors.New("asncap did not start within 30 s (see /tmp/asncap.log)")
}

// addTableLocal creates the CD (Change Data) table for a source table and
// registers it in ASNCDC.IBMSNAP_REGISTER.  This is the local-mode replacement
// for the ASNCDC.ADDTABLE SQL UDF which requires the IBM ASNTOOLS package.
//
// This function mirrors IBM's ASNCDC.ADDTABLE stored procedure:
//  1. ALTER TABLE … DATA CAPTURE CHANGES
//  2. CREATE TABLE ASNCDC."CDC_<schema>_<table>" with VARCHAR FOR BIT DATA cols,
//     UNIQUE INDEX, and VOLATILE CARDINALITY
//  3. INSERT INTO ASNCDC.IBMSNAP_REGISTER with STATE='A', NULL synchpoints,
//     ARCH_LEVEL='0801', OPTION_FLAGS='NNNN'
//  4. INSERT INTO ASNCDC.IBMSNAP_PRUNCNTL (subscriber entry required by asncap)
func addTableLocal(ctx context.Context, db *sql.DB, sourceSchema, sourceTable string) error {
	// IBM's ADDTABLE names the CD table "CDC_<schema>_<table>".
	cdTable := "CDC_" + sourceSchema + "_" + sourceTable

	// Check which parts of the setup are already in place.
	var cdExists, regExists int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM SYSCAT.TABLES WHERE TABSCHEMA='ASNCDC' AND TABNAME=?",
		cdTable,
	).Scan(&cdExists); err != nil {
		return fmt.Errorf("checking CD table existence: %w", err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=? AND SOURCE_TABLE=?",
		sourceSchema, sourceTable,
	).Scan(&regExists); err != nil {
		return fmt.Errorf("checking IBMSNAP_REGISTER for %s.%s: %w", sourceSchema, sourceTable, err)
	}

	// Both already present — check that the column types are correct (DB2 12
	// requires VARCHAR(16) FOR BIT DATA for IBMSNAP_COMMITSEQ; old containers
	// may have CHAR(10) from a previous version of this code).  If the types
	// are wrong, drop and recreate the CD table so asncap can INSERT 16-byte
	// commit sequence numbers without hitting SQLCODE -311.
	if cdExists > 0 && regExists > 0 {
		var commitseqType string
		var commitseqLen int
		_ = db.QueryRowContext(ctx,
			"SELECT TYPENAME, LENGTH FROM SYSCAT.COLUMNS WHERE TABSCHEMA='ASNCDC' AND TABNAME=? AND COLNAME='IBMSNAP_COMMITSEQ'",
			cdTable,
		).Scan(&commitseqType, &commitseqLen)

		if commitseqType == "VARCHAR" && commitseqLen == 16 {
			fmt.Printf("db2test: CD table ASNCDC.%s and registration already exist (correct schema)\n", cdTable)
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE %s.%s DATA CAPTURE CHANGES", sourceSchema, sourceTable),
			); err != nil {
				return fmt.Errorf("re-enabling DATA CAPTURE CHANGES on %s.%s: %w", sourceSchema, sourceTable, err)
			}
			return nil
		}

		// Wrong column type — drop and recreate.
		fmt.Printf("db2test: CD table ASNCDC.%s has wrong IBMSNAP_COMMITSEQ type (%s(%d)); dropping to recreate with VARCHAR(16)\n",
			cdTable, commitseqType, commitseqLen)
		_, _ = db.ExecContext(ctx, "DROP TABLE ASNCDC."+cdTable)
		_, _ = db.ExecContext(ctx,
			"DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=? AND SOURCE_TABLE=?",
			sourceSchema, sourceTable,
		)
		cdExists = 0
		regExists = 0
	}

	// Stale REGISTER row without a corresponding CD table: delete it so we can
	// recreate a consistent pair below.
	if regExists > 0 && cdExists == 0 {
		if _, err := db.ExecContext(ctx,
			"DELETE FROM ASNCDC.IBMSNAP_REGISTER WHERE SOURCE_OWNER=? AND SOURCE_TABLE=?",
			sourceSchema, sourceTable,
		); err != nil {
			return fmt.Errorf("removing stale IBMSNAP_REGISTER row for %s.%s: %w", sourceSchema, sourceTable, err)
		}
		fmt.Printf("db2test: removed stale IBMSNAP_REGISTER row for %s.%s\n", sourceSchema, sourceTable)
		regExists = 0
	}

	// Create CD table if missing.
	if cdExists == 0 {
		// Discover source table columns to mirror in the CD table.
		rows, err := db.QueryContext(ctx, `
			SELECT COLNAME, TYPENAME, LENGTH, SCALE
			FROM SYSCAT.COLUMNS
			WHERE TABSCHEMA = ? AND TABNAME = ?
			ORDER BY COLNO
		`, sourceSchema, sourceTable)
		if err != nil {
			return fmt.Errorf("querying source table columns for %s.%s: %w", sourceSchema, sourceTable, err)
		}
		defer rows.Close()

		type colDef struct {
			Name, TypeName string
			Len, Scale     int
		}
		var cols []colDef
		for rows.Next() {
			var c colDef
			if err := rows.Scan(&c.Name, &c.TypeName, &c.Len, &c.Scale); err != nil {
				return err
			}
			cols = append(cols, c)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(cols) == 0 {
			return fmt.Errorf("source table %s.%s has no columns or does not exist", sourceSchema, sourceTable)
		}

		// Build the CREATE TABLE DDL. All source columns are nullable in the CD
		// table because asncap writes only changed columns for UPDATE operations.
		var sb strings.Builder
		fmt.Fprintf(&sb, "CREATE TABLE ASNCDC.%s (\n", cdTable)
		sb.WriteString("  IBMSNAP_OPERATION CHAR(1) NOT NULL,\n")
		// DB2 12 uses VARCHAR(16) FOR BIT DATA for the 16-byte log LSN;
		// CHAR(10) causes SQLCODE -311 when asncap tries to INSERT.
		sb.WriteString("  IBMSNAP_COMMITSEQ VARCHAR(16) FOR BIT DATA NOT NULL,\n")
		sb.WriteString("  IBMSNAP_INTENTSEQ VARCHAR(16) FOR BIT DATA NOT NULL,\n")
		sb.WriteString("  IBMSNAP_LOGMARKER TIMESTAMP")
		for _, c := range cols {
			fmt.Fprintf(&sb, ",\n  %s %s", strings.TrimSpace(c.Name), db2TypeDef(c.TypeName, c.Len, c.Scale))
		}
		sb.WriteString("\n)")
		ddl := sb.String()
		fmt.Printf("db2test: creating CD table:\n%s\n", ddl)

		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("creating CD table ASNCDC.%s: %w", cdTable, err)
		}
	}

	// Enable DATA CAPTURE CHANGES on the source table. asncap requires this
	// attribute; without it it logs ASN0009E and stops. The ALTER is idempotent.
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("ALTER TABLE %s.%s DATA CAPTURE CHANGES", sourceSchema, sourceTable),
	); err != nil {
		return fmt.Errorf("enabling DATA CAPTURE CHANGES on %s.%s: %w", sourceSchema, sourceTable, err)
	}
	fmt.Printf("db2test: DATA CAPTURE CHANGES enabled on %s.%s\n", sourceSchema, sourceTable)

	// Create the unique index on (COMMITSEQ, INTENTSEQ) — required by IBM's ADDTABLE procedure.
	// Idempotent: isAlreadyExistsErr suppresses SQL0601N if the index already exists.
	indexSQL := fmt.Sprintf(
		`CREATE UNIQUE INDEX ASNCDC."IXCDC_%s_%s" ON ASNCDC."%s" (IBMSNAP_COMMITSEQ ASC, IBMSNAP_INTENTSEQ ASC) PCTFREE 0 MINPCTUSED 0`,
		sourceSchema, sourceTable, cdTable,
	)
	if _, err := db.ExecContext(ctx, indexSQL); err != nil && !isAlreadyExistsErr(err) {
		return fmt.Errorf("creating unique index on CD table ASNCDC.%s: %w", cdTable, err)
	}

	// VOLATILE CARDINALITY helps the DB2 optimizer use index scans on the CT table.
	volatileSQL := fmt.Sprintf(`ALTER TABLE ASNCDC."%s" VOLATILE CARDINALITY`, cdTable)
	if _, err := db.ExecContext(ctx, volatileSQL); err != nil {
		fmt.Printf("db2test: VOLATILE CARDINALITY warning (non-fatal): %v\n", err)
	}

	// Insert REGISTER row if missing.
	if regExists == 0 {
		// Key values required by asncap:
		//   ARCH_LEVEL  = '0801'  — the value asncap recognises regardless of DB2 version.
		//   OPTION_FLAGS = 'NNNN' — required; '0000' causes asncap to skip capture.
		//   CD_OLD_SYNCHPOINT = null, CD_NEW_SYNCHPOINT = null, SYNCHPOINT = null
		//                           — asncap sets these on reinit. Seeding with a stale
		//                             log position caused asncap to silently skip capture.
		//   STATE = 'A'           — Active at insert time; no separate UPDATE needed.
		//   CONFLICT_LEVEL = '0', CHG_UPD_TO_DEL_INS = 'Y', CHGONLY = 'N',
		//   RECAPTURE = 'Y'       — standard CDC capture flags.
		regSQL := fmt.Sprintf(`
			INSERT INTO ASNCDC.IBMSNAP_REGISTER (
				SOURCE_OWNER, SOURCE_TABLE, SOURCE_VIEW_QUAL, GLOBAL_RECORD,
				SOURCE_STRUCTURE, SOURCE_CONDENSED, SOURCE_COMPLETE,
				CD_OWNER, CD_TABLE, PHYS_CHANGE_OWNER, PHYS_CHANGE_TABLE,
				CD_OLD_SYNCHPOINT, CD_NEW_SYNCHPOINT,
				DISABLE_REFRESH,
				ARCH_LEVEL, CONFLICT_LEVEL, CHG_UPD_TO_DEL_INS,
				CHGONLY, RECAPTURE, OPTION_FLAGS,
				STOP_ON_ERROR, STATE, STATE_INFO
			) VALUES (
				'%s', '%s', 0, 'N',
				1, 'Y', 'Y',
				'ASNCDC', '%s', 'ASNCDC', '%s',
				null, null,
				0,
				'0801', '0', 'Y',
				'N', 'Y', 'NNNN',
				'Y', 'A', null
			)
		`, sourceSchema, sourceTable, cdTable, cdTable)

		if _, err := db.ExecContext(ctx, regSQL); err != nil {
			return fmt.Errorf("inserting IBMSNAP_REGISTER row for %s.%s: %w", sourceSchema, sourceTable, err)
		}
	}
	fmt.Printf("db2test: registered %s.%s → ASNCDC.%s (STATE=A, ARCH_LEVEL=0801, OPTION_FLAGS=NNNN)\n",
		sourceSchema, sourceTable, cdTable)

	// Step 4: Insert the IBMSNAP_PRUNCNTL subscriber row.
	//
	// asncap requires at least one PRUNCNTL entry for each registered source table
	// to know that a subscriber exists. Without this row asncap silently skips
	// capture for the table even though IBMSNAP_REGISTER STATE='A'.
	pruncntlSQL := fmt.Sprintf(`
		INSERT INTO ASNCDC.IBMSNAP_PRUNCNTL (
			TARGET_SERVER, TARGET_OWNER, TARGET_TABLE,
			SYNCHTIME, SYNCHPOINT,
			SOURCE_OWNER, SOURCE_TABLE, SOURCE_VIEW_QUAL,
			APPLY_QUAL, SET_NAME,
			CNTL_SERVER, TARGET_STRUCTURE, CNTL_ALIAS,
			PHYS_CHANGE_OWNER, PHYS_CHANGE_TABLE,
			MAP_ID
		) VALUES (
			'KAFKA', '%s', '%s',
			NULL, NULL,
			'%s', '%s', 0,
			'KAFKAQUAL', 'SET001',
			(SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1),
			8,
			(SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1),
			'ASNCDC', '%s',
			(SELECT CAST(COALESCE(MAX(CAST(MAP_ID AS INT)), 0) + 1 AS VARCHAR(10))
			 FROM ASNCDC.IBMSNAP_PRUNCNTL)
		)
	`, sourceSchema, sourceTable, sourceSchema, sourceTable, cdTable)

	if _, err := db.ExecContext(ctx, pruncntlSQL); err != nil {
		return fmt.Errorf("inserting IBMSNAP_PRUNCNTL row for %s.%s: %w", sourceSchema, sourceTable, err)
	}
	fmt.Printf("db2test: inserted IBMSNAP_PRUNCNTL subscriber row for %s.%s\n", sourceSchema, sourceTable)

	return nil
}

// db2TypeDef converts SYSCAT.COLUMNS metadata to a SQL type definition string.
func db2TypeDef(typename string, length, scale int) string {
	switch strings.ToUpper(strings.TrimSpace(typename)) {
	case "INTEGER", "INT", "BIGINT", "SMALLINT", "REAL", "DOUBLE", "DATE", "TIME", "TIMESTAMP", "BOOLEAN":
		return strings.ToUpper(strings.TrimSpace(typename))
	case "CHARACTER", "CHAR":
		return fmt.Sprintf("CHAR(%d)", length)
	case "VARCHAR", "CHARACTER VARYING":
		return fmt.Sprintf("VARCHAR(%d)", length)
	case "DECIMAL", "NUMERIC":
		return fmt.Sprintf("DECIMAL(%d,%d)", length, scale)
	case "FLOAT":
		return fmt.Sprintf("FLOAT(%d)", length)
	case "CLOB":
		return fmt.Sprintf("CLOB(%d)", length)
	case "BLOB":
		return fmt.Sprintf("BLOB(%d)", length)
	default:
		return strings.TrimSpace(typename)
	}
}
