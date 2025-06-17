// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build unix

package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	dirs := []string{
		"/bin/",
		"/dev/",
		"/etc/",
		"/etc/ssl/certs/",
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
		"/etc/ssl/certs/ca-certificates.crt",
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(filepath.Join(chrootDir, dir), 0o755); err != nil {
			return fmt.Errorf("create %s directory: %w", dir, err)
		}
	}
	for _, filePath := range configFiles {
		if err := copyFile(filePath, filepath.Join(chrootDir, filePath)); err != nil {
			return fmt.Errorf("copy %s: %w", filePath, err)
		}
	}

	// Recursively make chroot directory read-only
	if err := makeReadOnly(chrootDir); err != nil {
		return fmt.Errorf("make directory read-only: %w", err)
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	stat, err := srcFile.Stat()
	if err != nil {
		return err
	}

	dstFile, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, stat.Mode())
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return nil
}

func makeReadOnly(root string) error {
	return filepath.Walk(root, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		return os.Chmod(filePath, info.Mode() & ^os.FileMode(0o222))
	})
}
