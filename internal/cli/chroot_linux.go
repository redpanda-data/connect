// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build linux

package cli

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// chroot creates read-only empty directory containing only required /etc files,
// and chroots into it. The directory must not exist before calling this
// function.
//
// NOTE: This function will only work if the binary is running with
// sufficient privileges to call syscall.Chroot. If the binary does not
// have the necessary privileges, this function will return an error.
func chroot(path string) error {
	if err := setupChrootDir(path); err != nil {
		return fmt.Errorf("setup chroot: %w", err)
	}

	if err := syscall.Chroot(path); err != nil {
		return err
	}
	if err := syscall.Chdir("/"); err != nil {
		return err
	}

	return nil
}

func setupChrootDir(chrootDir string) error {
	// Make sure chroot directory does not exist
	if _, err := os.Stat(chrootDir); err == nil {
		return fmt.Errorf("chroot directory %s must not exist", chrootDir)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("check directory: %w", err)
	}

	// Create UNIX directory structure, and copy required /etc files
	directories := []string{
		"/bin/",
		"/dev/",
		"/etc/",
		"/home/",
		"/lib/",
		"/proc/",
		"/root/",
		"/sys/",
		"/tmp/",
		"/usr/",
		"/usr/bin/",
		"/usr/sbin/",
		"/var/",
		"/var/spool/",
	}
	configFiles := []string{
		"/etc/group",
		"/etc/hostname",
		"/etc/hosts",
		"/etc/nsswitch.conf",
		"/etc/passwd",
		"/etc/resolv.conf",
	}
	for _, dir := range directories {
		if err := os.MkdirAll(filepath.Join(chrootDir, dir), 0o755); err != nil {
			return fmt.Errorf("create %s directory: %w", dir, err)
		}
	}
	for _, filePath := range configFiles {
		if err := copyFile(filePath, filepath.Join(chrootDir, filePath)); err != nil {
			return fmt.Errorf("copy %s: %w", filePath, err)
		}
	}

	// Copy present TLS/SSL certificates - based on root_linux.go [1].
	//
	// I also tired forcing loading of system CA certificates instead of copying
	// them, but it does not work in all cases.
	//
	// [1] https://github.com/golang/go/blob/master/src/crypto/x509/root_linux.go.
	certFiles := []string{
		"/etc/ssl/certs/ca-certificates.crt",                // Debian/Ubuntu/Gentoo etc.
		"/etc/pki/tls/certs/ca-bundle.crt",                  // Fedora/RHEL 6
		"/etc/ssl/ca-bundle.pem",                            // OpenSUSE
		"/etc/pki/tls/cacert.pem",                           // OpenELEC
		"/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem", // CentOS/RHEL 7
		"/etc/ssl/cert.pem",                                 // Alpine Linux
	}
	certDirectories := []string{
		"/etc/ssl/certs",     // SLES10/SLES11, https://golang.org/issue/12139
		"/etc/pki/tls/certs", // Fedora/RHEL
	}
	for _, filePath := range certFiles {
		if err := maybeCopyFile(filePath, filepath.Join(chrootDir, filePath)); err != nil {
			return fmt.Errorf("copy %s: %w", filePath, err)
		}
	}
	for _, dirPath := range certDirectories {
		if err := maybeCopyDir(dirPath, filepath.Join(chrootDir, dirPath)); err != nil {
			return fmt.Errorf("copy directory %s: %w", dirPath, err)
		}
	}

	// Recursively make chroot directory read-only
	if err := makeReadOnly(chrootDir); err != nil {
		return fmt.Errorf("make directory read-only: %w", err)
	}

	return nil
}

func maybeCopyFile(src, dst string) error {
	err := copyFile(src, dst)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("create parent directory: %w", err)
	}

	dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, srcInfo.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return nil
}

func maybeCopyDir(src, dst string) error {
	entries, err := readUniqueDirectoryEntries(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Ignore if directory doesn't exist
		}
		return err
	}

	if err := os.MkdirAll(dst, 0o0755); err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip subdirectories
		}

		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if err := copyFile(srcPath, dstPath); err != nil {
			return err
		}
	}

	return nil
}

// readUniqueDirectoryEntries is like os.ReadDir but omits
// symlinks that point within the directory.
func readUniqueDirectoryEntries(dir string) ([]fs.DirEntry, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	uniq := files[:0]
	for _, f := range files {
		if !isSameDirSymlink(f, dir) {
			uniq = append(uniq, f)
		}
	}
	return uniq, nil
}

// isSameDirSymlink reports whether fi in dir is a symlink with a
// target not containing a slash.
func isSameDirSymlink(f fs.DirEntry, dir string) bool {
	if f.Type()&fs.ModeSymlink == 0 {
		return false
	}
	target, err := os.Readlink(filepath.Join(dir, f.Name()))
	return err == nil && !strings.Contains(target, "/")
}

func makeReadOnly(root string) error {
	return filepath.Walk(root, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chmod(filePath, info.Mode() & ^os.FileMode(0o222))
	})
}
