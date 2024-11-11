// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

//go:build unix

package ollama

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/connect/v4/internal/singleton"
)

// lookPath is very similar to exec.LookPath, except that it uses service.FS filesystem
// abstractions.
func (c *runOllamaConfig) lookPath(file string) (string, error) {
	path := os.Getenv("PATH")
	for _, dir := range filepath.SplitList(path) {
		if !filepath.IsAbs(path) {
			continue
		}
		path := filepath.Join(dir, file)
		p, err := c.fs.Stat(path)
		if err != nil || p.IsDir() {
			continue
		}
		// Check that the file is executable
		if err := syscall.Access(path, 0x1); err == nil {
			return path, nil
		}
	}
	return "", exec.ErrNotFound
}

func (c *runOllamaConfig) downloadOllama(ctx context.Context, path string) error {
	var url string
	if c.downloadURL == "" {
		const baseURL string = "https://github.com/ollama/ollama/releases/download/v0.4.0/ollama"
		switch runtime.GOOS {
		case "darwin":
			// They ship an universal executable for darwin
			url = baseURL + "-darwin"
		case "linux":
			url = fmt.Sprintf("%s-%s-%s.tgz", baseURL, runtime.GOOS, runtime.GOARCH)
		default:
			return fmt.Errorf("automatic download of ollama is not supported on %s, please download ollama manually", runtime.GOOS)
		}
	} else {
		url = c.downloadURL
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to download ollama binary: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download ollama binary: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("failed to download ollama binary: status_code=%d", resp.StatusCode)
	}
	var binary io.Reader = resp.Body
	if strings.HasSuffix(url, ".tgz") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return fmt.Errorf("unable to read tarball for ollama binary download: %w", err)
		}
		reader := tar.NewReader(gz)
		for {
			header, err := reader.Next()
			if err == io.EOF {
				return fmt.Errorf("unable to find ollama binary within tarball at %s", url)
			} else if err != nil {
				return fmt.Errorf("unable to read tarball at %s: %w", url, err)
			}
			if !header.FileInfo().Mode().IsRegular() || header.Name != "./bin/ollama" {
				continue
			}
			binary = reader
			break
		}
	}
	ollama, err := c.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	if err != nil {
		return fmt.Errorf("unable to create file for ollama binary download: %w", err)
	}
	defer ollama.Close()
	w, ok := (ollama).(io.Writer)
	if !ok {
		return errors.New("unable to download ollama binary to filesystem")
	}
	_, err = io.Copy(w, binary)
	return err
}

var ollamaProcess = singleton.New(singleton.Config[*exec.Cmd]{
	Constructor: func(ctx context.Context) (*exec.Cmd, error) {
		cfg, ok := ctx.Value(configKey).(runOllamaConfig)
		if !ok {
			return nil, errors.New("missing config")
		}
		serverPath, err := cfg.lookPath("ollama")
		if errors.Is(err, exec.ErrNotFound) {
			serverPath = path.Join(cfg.cacheDir, "ollama")
			if err := os.MkdirAll(cfg.cacheDir, 0777); err != nil {
				return nil, err
			}
			if _, err = os.Stat(serverPath); errors.Is(err, os.ErrNotExist) {
				err = backoff.Retry(func() error {
					cfg.logger.Infof("downloading ollama to %s", serverPath)
					return cfg.downloadOllama(ctx, serverPath)
				}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3))
			}
			if err != nil {
				return nil, err
			}
			cfg.logger.Info("ollama download complete")
		} else if err != nil {
			return nil, err
		}
		cfg.logger.Tracef("starting ollama subprocess at %s", serverPath)
		proc := exec.Command(serverPath, "serve")
		proc.Env = append(os.Environ(), "OLLAMA_MODELS="+cfg.cacheDir)
		proc.Stdout = &commandOutput{logger: cfg.logger}
		proc.Stderr = &commandOutput{logger: cfg.logger}
		if err = proc.Start(); err != nil {
			return nil, err
		}
		return proc, nil
	},
	Destructor: func(ctx context.Context, cmd *exec.Cmd) error {
		if cmd.Process == nil {
			return nil
		}
		if err := cmd.Process.Signal(os.Interrupt); err != nil {
			return err
		}
		// Wait for 3 seconds for the process to exit gracefully then kill it by force.
		stop := make(chan any, 1)
		wg := errgroup.Group{}
		wg.Go(func() error {
			err := cmd.Wait()
			stop <- struct{}{}
			return err
		})
		wg.Go(func() error {
			select {
			case <-stop:
			case <-time.After(3 * time.Second):
				// Ignore errors if there is a race
				// and the process has already closed.
				_ = cmd.Process.Kill()
			}
			return nil
		})
		return wg.Wait()
	}})
