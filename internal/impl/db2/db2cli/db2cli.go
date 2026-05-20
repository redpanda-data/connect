// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2cli

import (
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/ebitengine/purego"
)

// DB2 CLI ("Call Level Interface") library handle. "CLI" is IBM's term for
// its C-based ODBC-compatible SQL API — not a command-line tool. The name
// appears in library filenames (libdb2.so, db2cli.dll) and IBM documentation.
var (
	// libHandle is written once inside loadOnce.Do; visible after libLoaded is true.
	libHandle uintptr
	// libLoaded is set to true after a successful load; provides acquire/release fence.
	libLoaded atomic.Bool
	loadOnce  sync.Once
	// loadErr is written once inside loadOnce.Do; safe to read via LoadLibrary return value.
	loadErr error
)

// getLibraryPath returns the platform-specific DB2 CLI shared library name.
// "CLI" here is IBM's Call Level Interface — a C-based ODBC-compatible API
// for executing SQL against DB2, not a command-line tool.
// IBM ships libdb2.so.1 for Linux x86_64 and ppc64le; libdb2.dylib for macOS
// (Intel/ARM via Rosetta). Windows ships db2cli.dll. These are the only
// platforms tested with the IBM DB2 client. Any other GOOS is unsupported and
// panics at init time rather than failing silently at query time.
func getLibraryPath() string {
	switch runtime.GOOS {
	case "linux":
		return "libdb2.so.1" // resolved via LD_LIBRARY_PATH or ldconfig cache
	case "darwin":
		return "libdb2.dylib"
	case "windows":
		return "db2cli.dll"
	default:
		panic("db2cli: unsupported GOOS " + runtime.GOOS + "; IBM DB2 CLI is only available on linux, darwin, and windows")
	}
}

// LoadLibrary loads the IBM DB2 CLI shared library using the platform's
// standard dynamic linker search (LD_LIBRARY_PATH / ldconfig cache on Linux,
// DYLD_LIBRARY_PATH on macOS). The library name is platform-specific:
//   - Linux:   libdb2.so.1  (IBM Data Server Driver package)
//   - macOS:   libdb2.dylib (IBM ODBC CLI Driver / clidriver)
//   - Windows: db2cli.dll
//
// LoadLibrary is idempotent: subsequent calls return the cached result of the
// first attempt. If the library cannot be found, the error message includes
// installation hints. For tests that extract the library from a Docker container
// use LoadLibraryFromPath to bypass the linker search.
func LoadLibrary() error {
	return loadLibrary(getLibraryPath())
}

// LoadLibraryFromPath loads the IBM DB2 CLI shared library from an absolute
// file system path, bypassing the platform linker cache. This is used by
// integration tests that extract libdb2.so.1 from the DB2 container image into
// a known directory (see db2test.ensureDB2Library). The load is still idempotent
// — if a previous LoadLibrary or LoadLibraryFromPath call succeeded, this is a
// no-op. Returns an error if the library cannot be opened or if any function
// symbol fails to resolve.
//
// path must be an absolute path. Relative paths and paths with ".." traversal
// components are rejected to prevent unintended library substitution.
func LoadLibraryFromPath(path string) error {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return fmt.Errorf("LoadLibraryFromPath requires an absolute path, got %q", path)
	}
	return loadLibrary(path)
}

func loadLibrary(libPath string) error {
	loadOnce.Do(func() {
		// RTLD_LOCAL (not RTLD_GLOBAL) prevents DB2 CLI symbols from leaking into
		// the global symbol namespace, which would allow a malicious libdb2.so on
		// LD_LIBRARY_PATH to hijack symbols used by other libraries.
		libHandle, loadErr = purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_LOCAL)
		if loadErr != nil {
			loadErr = fmt.Errorf("loading DB2 CLI library %s: %w (ensure DB2 client is installed and library is in PATH/LD_LIBRARY_PATH)", libPath, loadErr)
			return
		}

		loadErr = registerFunctions()
		if loadErr != nil {
			loadErr = fmt.Errorf("registering DB2 CLI functions: %w", loadErr)
			return
		}
		// libLoaded.Store is a release fence: any goroutine that reads
		// libLoaded.Load() == true is guaranteed to observe the writes to
		// libHandle and all registered function pointers above.
		libLoaded.Store(true)
	})

	return loadErr
}

// registerFunctions registers all DB2 CLI function pointers
func registerFunctions() error {
	// This will register function pointers defined in functions.go
	// Each function will be registered individually using purego.RegisterLibFunc

	if err := registerConnectionFunctions(); err != nil {
		return fmt.Errorf("register connection functions: %w", err)
	}

	if err := registerStatementFunctions(); err != nil {
		return fmt.Errorf("register statement functions: %w", err)
	}

	if err := registerResultFunctions(); err != nil {
		return fmt.Errorf("register result functions: %w", err)
	}

	if err := registerTransactionFunctions(); err != nil {
		return fmt.Errorf("register transaction functions: %w", err)
	}

	if err := registerDiagnosticFunctions(); err != nil {
		return fmt.Errorf("register diagnostic functions: %w", err)
	}

	if err := registerMetadataFunctions(); err != nil {
		return fmt.Errorf("register metadata functions: %w", err)
	}

	if err := registerCursorFunctions(); err != nil {
		return fmt.Errorf("register cursor functions: %w", err)
	}

	if err := registerDataAtExecFunctions(); err != nil {
		return fmt.Errorf("register data-at-exec functions: %w", err)
	}

	if err := registerExtendedConnFunctions(); err != nil {
		return fmt.Errorf("register extended connection functions: %w", err)
	}

	if err := registerDescriptorFunctions(); err != nil {
		return fmt.Errorf("register descriptor functions: %w", err)
	}

	return nil
}

// IsLoaded reports whether the DB2 CLI shared library was loaded successfully
// and all function pointers were registered. Returns false if LoadLibrary has
// not been called yet or if the last call returned an error.
func IsLoaded() bool {
	// libLoaded.Load() is an acquire fence: if it returns true, all writes made
	// by loadLibrary (libHandle, function pointer registrations) are visible.
	return libLoaded.Load()
}

// GetLibraryHandle returns the raw dlopen handle for the loaded DB2 CLI shared
// library. This is an opaque uintptr that can be passed to purego.RegisterLibFunc
// for additional function registrations not covered by this package. Returns 0
// if the library has not been loaded.
func GetLibraryHandle() uintptr {
	// libLoaded.Load() ensures libHandle is visible if the library was loaded.
	if !libLoaded.Load() {
		return 0
	}
	return libHandle
}
