// Copyright 2025 Redpanda Data, Inc.
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

package git

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"github.com/Jeffail/shutdown"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// Ensure input implements service.Input at compile time.
var _ service.Input = (*input)(nil)

// input implements a service.Input that reads files from a Git repository.
// It clones the repository, monitors for changes, and emits file contents as messages.
type input struct {
	// cfg contains all config parameters for this input.
	cfg inputCfg
	// log is the logger instance for this input.
	log *service.Logger
	// filesChan is used to send file details from the scanner to the reader.
	filesChan chan fileEvent
	// errorChan is used to send errors form the scanner to the reader.
	errorChan chan error
	// shutSig signals when the input should stop processing.
	shutSig *shutdown.Signaller
	// repository is the Git repository instance.
	repository *git.Repository
	// lastCommit is the hash of the most recently processed commit.
	lastCommit plumbing.Hash
	// lastCommitMu is a lock for accessing lastCommit.
	lastCommitMu sync.RWMutex
	// tempDir is the temporary directory where the repository is cloned.
	tempDir string
	// mgr is the service resources manager.
	mgr *service.Resources
}

// fileEvent represents a file change event.
type fileEvent struct {
	// path is the absolute path to the file.
	path string
	// isDeleted indicates whether the file was deleted.
	isDeleted bool
	// ackFn is the function to call when the file is acknowledged.
	ackFn func()
}

// init registers the Git input plugin with the service registry.
func init() {
	err := service.RegisterInput(
		"git", gitInputConfig(),
		func(parsedCfg *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			conf, err := inputCfgFromParsed(parsedCfg)
			if err != nil {
				return nil, err
			}

			return service.AutoRetryNacksToggled(parsedCfg, newInput(conf, mgr))
		})
	if err != nil {
		panic(err)
	}
}

// newInput creates a new Git input instance from a parsed configuration.
func newInput(cfg inputCfg, mgr *service.Resources) *input {
	return &input{
		cfg:       cfg,
		filesChan: make(chan fileEvent),
		errorChan: make(chan error),
		shutSig:   nil,
		log:       mgr.Logger(),
		mgr:       mgr,
	}
}

// Connect implements service.Input. It initializes the Git repository by creating
// a temporary directory, cloning the repository, and starting the polling routine.
func (in *input) Connect(ctx context.Context) error {
	// On reconnect wait for previous process to shutdown
	if in.shutSig != nil {
		select {
		case <-in.shutSig.HasStoppedChan():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	in.shutSig = shutdown.NewSignaller()
	in.filesChan = make(chan fileEvent)
	in.errorChan = make(chan error)
	// Create a temporary directory for the repository
	tmpDir, err := os.MkdirTemp("", "git-input-*")
	if err != nil {
		in.shutSig.TriggerHasStopped()
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	in.tempDir = tmpDir

	// If checkpoint cache is configured, try to get the last processed commit
	var cachedCommitHash plumbing.Hash
	if in.cfg.checkpointCache != "" {
		if err := in.mgr.AccessCache(ctx, in.cfg.checkpointCache, func(cache service.Cache) {
			lastCommitBytes, cacheErr := cache.Get(ctx, in.cfg.checkpointKey)
			if cacheErr != nil && !errors.Is(cacheErr, service.ErrKeyNotFound) {
				err = fmt.Errorf("failed to get last commit from cache: %w", cacheErr)
				return
			}
			cachedCommitHash = plumbing.NewHash(string(lastCommitBytes))
		}); err != nil {
			in.shutSig.TriggerHasStopped()
			return err
		}

		if cachedCommitHash != plumbing.ZeroHash {
			in.log.Infof("continuing from cached last commit: %q", cachedCommitHash)
			in.lastCommitMu.Lock()
			in.lastCommit = cachedCommitHash
			in.lastCommitMu.Unlock()
		}
	}

	// Clone the repository
	if err := in.cloneRepo(ctx); err != nil {
		_ = os.RemoveAll(tmpDir)
		in.shutSig.TriggerHasStopped()
		return fmt.Errorf("failed to clone repo: %w", err)
	}

	// Start polling for changes, cleanup when we're done
	go func() {
		ctx, cancel := in.shutSig.SoftStopCtx(context.Background())
		defer cancel()
		defer close(in.filesChan)
		defer close(in.errorChan)
		defer in.shutSig.TriggerHasStopped()
		in.pollChanges(ctx, cachedCommitHash)
		if in.tempDir != "" {
			if err := os.RemoveAll(in.tempDir); err != nil {
				in.log.Errorf("Failed to remove temp directory: %v", err)
			}
		}
	}()

	return nil
}

// Read implements service.Input. It returns the next available file content as a message,
// or returns an error if the context is cancelled or shutdown is signaled.
func (in *input) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case err, ok := <-in.errorChan:
			if !ok {
				return nil, nil, service.ErrNotConnected
			}
			return nil, nil, err
		case event, ok := <-in.filesChan:
			if !ok {
				return nil, nil, service.ErrNotConnected
			}
			if event.isDeleted {
				// For deleted files, create a message with empty content and metadata
				msg := service.NewMessage(nil)
				relPath, err := filepath.Rel(in.tempDir, event.path)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to get relative path for %s: %w", event.path, err)
				}
				msg.MetaSet("git_file_path", relPath)
				msg.MetaSet("git_commit", in.getLastCommit().String())
				msg.MetaSetMut("git_deleted", true)
				return msg, func(ctx context.Context, _ error) error { event.ackFn(); return nil }, nil
			}

			msg, err := in.createMessage(event.path)
			if err != nil {
				return nil, nil, err
			}

			// If createMessage returns nil, nil, it means we should skip this file
			if msg == nil {
				continue // Skip this file and read the next one
			}

			return msg, func(ctx context.Context, _ error) error { event.ackFn(); return nil }, nil
		}
	}
}

// Close implements service.Input. It signals shutdown and cleans up the temporary repository directory.
func (in *input) Close(ctx context.Context) error {
	if in.shutSig == nil {
		return nil
	}
	in.shutSig.TriggerHardStop()
	select {
	case <-in.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// cloneRepo clones the configured Git repository into the temporary directory and
// sets the initial commit hash.
func (in *input) cloneRepo(ctx context.Context) error {
	auth, err := in.setupAuth()
	if err != nil {
		return err
	}

	in.repository, err = git.PlainClone(in.tempDir, false, &git.CloneOptions{
		URL:           in.cfg.repoURL,
		Auth:          auth,
		ReferenceName: plumbing.NewBranchReferenceName(in.cfg.branch),
		SingleBranch:  true,
		Depth:         1,
	})
	if err != nil {
		return fmt.Errorf("git clone failed: %w", err)
	}
	ref, err := in.repository.Head()
	if err != nil {
		return fmt.Errorf("unable to get reference: %w", err)
	}
	in.lastCommitMu.Lock()
	in.lastCommit = ref.Hash()
	in.lastCommitMu.Unlock()
	return nil
}

// setLastCommit sets the in.lastCommit field and updates the checkpoint cache (if configured).
func (in *input) setLastCommit(ctx context.Context, newCommit plumbing.Hash) {
	in.lastCommitMu.Lock()
	in.lastCommit = newCommit
	in.lastCommitMu.Unlock()

	if in.cfg.checkpointCache == "" {
		return
	}
	if err := in.updateCheckpointCache(ctx, newCommit); err != nil {
		in.log.Errorf("failed to update checkpoint cache: %v", err)
	}
}

// getLastCommit retrieves the lastCommit we pulled in a concurrent safe way.
func (in *input) getLastCommit() plumbing.Hash {
	in.lastCommitMu.RLock()
	defer in.lastCommitMu.RUnlock()
	return in.lastCommit
}

// pollChanges runs in a separate goroutine and periodically checks for updates
// in the Git repository according to the configured poll interval.
func (in *input) pollChanges(ctx context.Context, cachedCommit plumbing.Hash) {
	hasCheckpoint := cachedCommit != plumbing.ZeroHash
	var initialScanWg *sync.WaitGroup
	if hasCheckpoint {
		// Perform initial catch-up
		wg, err := in.processChangedFiles(ctx, cachedCommit, in.getLastCommit())
		if err != nil {
			select {
			case in.errorChan <- fmt.Errorf("error on initial catch up: %w", err):
			case <-ctx.Done():
			}
			return
		}
		initialScanWg = wg
	} else {
		// Otherwise, do a full initial scan of the repo
		wg, err := in.walkRepositoryFiles(ctx)
		if err != nil {
			err = fmt.Errorf("initial file scan error: %w", err)
			select {
			case in.errorChan <- err:
			case <-ctx.Done():
			}
			return
		}
		initialScanWg = wg
	}

	done := make(chan any)
	go func() {
		initialScanWg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return
	}

	in.setLastCommit(ctx, in.getLastCommit())

	ticker := time.NewTicker(in.cfg.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := in.fetchAndProcessNewCommits(ctx); err != nil {
				err = fmt.Errorf("failed to check for updates: %v", err)
				select {
				case in.errorChan <- err:
				case <-ctx.Done():
				}
				return
			}
		}
	}
}

// fetchAndProcessNewCommits pulls the latest changes from the repository and triggers
// a scan of changed files if the commit hash has changed.
func (in *input) fetchAndProcessNewCommits(ctx context.Context) error {
	in.log.Debug("fetching new commits and processing changes")
	// Store the current commit before pull
	oldCommit := in.getLastCommit()

	// Fetch and pull changes
	wt, err := in.repository.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	auth, err := in.setupAuth()
	if err != nil {
		return err
	}

	in.log.Debugf("Pulling repository...")
	if err := in.pullGitChanges(ctx, wt, auth); err != nil {
		in.log.Debugf("Pull returned: %v", err)
		return err
	}
	in.log.Debugf("Pull done.")

	// Get the new HEAD reference
	ref, err := in.repository.Head()
	if err != nil {
		return fmt.Errorf("failed to get HEAD reference: %w", err)
	}

	newCommit := ref.Hash()
	if newCommit == oldCommit {
		in.log.Debugf("no changes detected since last commit")
		return nil
	}

	// If the commit hash has changed, process the changes
	wg, err := in.processChangedFiles(ctx, oldCommit, newCommit)
	if err != nil {
		return fmt.Errorf("failed to process changed files: %w", err)
	}

	done := make(chan any)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		in.setLastCommit(ctx, newCommit)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// updateCheckpointCache writes the new commit hash into the cache, if configured.
// We log errors but do not necessarily return them as fatal, so the rest of
// the pipeline can continue.
func (in *input) updateCheckpointCache(ctx context.Context, newHash plumbing.Hash) error {
	if in.cfg.checkpointCache == "" {
		return nil
	}
	in.log.Debugf("updating checkpoint cache to commit %q", newHash)

	return in.mgr.AccessCache(ctx, in.cfg.checkpointCache, func(cache service.Cache) {
		if err := cache.Set(ctx, in.cfg.checkpointKey, []byte(newHash.String()), nil); err != nil {
			in.log.Errorf("failed to update checkpoint cache: %v", err)
		}
	})
}

// pullGitChanges attempts to pull the latest changes from the remote.
// If there's no update, it returns nil.
func (*input) pullGitChanges(ctx context.Context, wt *git.Worktree, auth transport.AuthMethod) error {
	err := wt.PullContext(ctx, &git.PullOptions{
		RemoteName: "origin",
		Auth:       auth,
		Force:      true,
	})
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("git pull failed: %w", err)
	}
	return nil
}

// processChangedFiles identifies changes between two commits and processes them.
func (in *input) processChangedFiles(ctx context.Context, oldCommit, newCommit plumbing.Hash) (*sync.WaitGroup, error) {
	// Get the old and new commit objects
	oldCommitObj, err := in.repository.CommitObject(oldCommit)
	if err != nil {
		return nil, fmt.Errorf("failed to get old commit object: %w", err)
	}

	newCommitObj, err := in.repository.CommitObject(newCommit)
	if err != nil {
		return nil, fmt.Errorf("failed to get new commit object: %w", err)
	}

	// Compare the two commits
	diff, err := oldCommitObj.Patch(newCommitObj)
	if err != nil {
		return nil, fmt.Errorf("failed to generate diff: %w", err)
	}

	wg := &sync.WaitGroup{}

	// Process each changed file
	for _, filePatch := range diff.FilePatches() {
		from, to := filePatch.Files()
		hasBeenDeleted := from != nil && to == nil
		hasBeenAddedOrModified := to != nil

		if hasBeenDeleted {
			path := filepath.Join(in.tempDir, from.Path())
			relPath := from.Path()

			// Check patterns
			if in.matchesPatterns(relPath) {
				wg.Add(1)
				select {
				case in.filesChan <- fileEvent{path: path, isDeleted: true, ackFn: wg.Done}:
				case <-ctx.Done():
					wg.Done()
					return nil, ctx.Err()
				}
			}
			continue
		}

		if hasBeenAddedOrModified {
			path := filepath.Join(in.tempDir, to.Path())
			relPath := to.Path()

			// Check patterns
			if in.matchesPatterns(relPath) {
				wg.Add(1)
				select {
				case in.filesChan <- fileEvent{path: path, isDeleted: false, ackFn: wg.Done}:
				case <-ctx.Done():
					wg.Done()
					return nil, ctx.Err()
				}
			}
		}
	}

	in.log.Debugf("processed changes, found %d file changes", len(diff.FilePatches()))

	return wg, nil
}

// matchesPatterns checks if the relative path matches the include/exclude patterns.
func (in *input) matchesPatterns(relPath string) bool {
	// Check exclude patterns first
	for _, pattern := range in.cfg.excludePatterns {
		if matched, err := doublestar.PathMatch(pattern, relPath); err == nil && matched {
			return false
		}
	}

	// If no include patterns, include all files
	if len(in.cfg.includePatterns) == 0 {
		return true
	}

	// Check include patterns
	for _, pattern := range in.cfg.includePatterns {
		if matched, err := doublestar.PathMatch(pattern, relPath); err == nil && matched {
			return true
		}
	}
	return false
}

// walkRepositoryFiles walks through the repository directory, applying include/exclude patterns,
// and sends matching file paths to the files channel for processing.
func (in *input) walkRepositoryFiles(ctx context.Context) (*sync.WaitGroup, error) {
	scanPath := in.tempDir

	wg := &sync.WaitGroup{}
	err := filepath.WalkDir(scanPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// We need to recurse into directories, but aren't interested in directories itself
		if d.IsDir() {
			return nil
		}

		// Get relative path for pattern matching
		relPath, err := filepath.Rel(scanPath, path)
		if err != nil {
			return err
		}

		// Check patterns
		if in.matchesPatterns(relPath) {
			wg.Add(1)
			select {
			case in.filesChan <- fileEvent{path: path, isDeleted: false, ackFn: wg.Done}:
			case <-ctx.Done():
				wg.Done()
				return ctx.Err()
			}
		}
		return nil
	})
	return wg, err
}

// detectMimeType determines the MIME type of a file by examining its contents or by looking
// at the file name's extension.
func (in *input) detectMimeType(filePath string) (string, bool) {
	// Read the first 512 bytes of the file for MIME detection
	f, err := os.Open(filePath)
	if err != nil {
		in.log.Warnf("failed to open file %q for MIME detection: %v. Using fallback application/octet-stream.", filePath, err)
		return "application/octet-stream", false
	}
	defer f.Close()

	buffer := make([]byte, 512)
	n, err := f.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		in.log.Warnf("failed to read file %q for MIME detection: %v. Using error fallback application/octet-stream.", filePath, err)
		return "application/octet-stream", false
	}

	// Detect content type and check if binary
	contentTypeWithMetadata := http.DetectContentType(buffer[:n])
	contentType := strings.Split(contentTypeWithMetadata, ";")[0]

	ext := strings.ToLower(filepath.Ext(filePath))
	if mimeType, exists := extensionToMIME[ext]; exists {
		contentType = mimeType
	}

	isBinary := isBinaryMIME(contentType)

	return contentType, isBinary
}

// createMessage reads the content of a file and creates a new message.
// If includeInfo is enabled, it also adds file metadata to the message.
func (in *input) createMessage(filePath string) (*service.Message, error) {
	relPath, err := filepath.Rel(in.tempDir, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get relative path for %s: %w", filePath, err)
	}

	// Get file info
	info, err := os.Lstat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %w", relPath, err)
	}

	if info.Mode()&os.ModeSymlink != 0 {
		in.log.Debugf("skipping symbolic link %s", relPath)
		return nil, nil
	}

	// Detect MIME type and binary status
	mimeType, isBinary := in.detectMimeType(filePath)

	// Check file size limit for binary files
	isLimitSet := in.cfg.maxFileSize > 0
	isWithinSizeLimit := isLimitSet && info.Size() > int64(in.cfg.maxFileSize)
	if isWithinSizeLimit {
		in.log.Debugf("skipping large binary file %s (size: %d, limit: %d)",
			filePath, info.Size(), in.cfg.maxFileSize)
		return nil, nil
	}

	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", relPath, err)
	}

	msg := service.NewMessage(content)

	// Add file metadata
	hashValue := sha256.Sum256(content)
	hashStr := hex.EncodeToString(hashValue[:])
	msg.MetaSet("git_file_content_hash", hashStr)
	msg.MetaSet("git_file_path", relPath)
	msg.MetaSetMut("git_file_size", info.Size())
	msg.MetaSet("git_file_mode", fmt.Sprintf("%o", info.Mode()))
	msg.MetaSetMut("git_file_modified", info.ModTime())
	msg.MetaSet("git_commit", in.getLastCommit().String())
	msg.MetaSet("git_mime_type", mimeType)
	msg.MetaSetMut("git_is_binary", isBinary)

	return msg, nil
}

// setupAuth configures and returns the appropriate authentication method based on the configuration.
func (in *input) setupAuth() (transport.AuthMethod, error) {
	// Check if basic auth is configured
	if in.cfg.auth.basic.username != "" {
		return &githttp.BasicAuth{
			Username: in.cfg.auth.basic.username,
			Password: in.cfg.auth.basic.password,
		}, nil
	}

	// Check if token auth is configured
	if in.cfg.auth.token.value != "" {
		// Check the repository URL to determine how to use the token
		repoURL, err := url.Parse(in.cfg.repoURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse repository URL: %w", err)
		}

		// For GitHub, use the token as the password with an empty username
		if strings.Contains(repoURL.Host, "github.com") {
			return &githttp.BasicAuth{
				Username: "", // Not needed for GitHub tokens
				Password: in.cfg.auth.token.value,
			}, nil
		}

		// For GitLab and others the username should not be blank or equal to "oauth2"
		return &githttp.BasicAuth{
			Username: "oauth2",
			Password: in.cfg.auth.token.value,
		}, nil
	}

	// Check if SSH key auth is configured
	if in.cfg.auth.sshKey.privateKey != "" || in.cfg.auth.sshKey.privateKeyPath != "" {
		var publicKeys *ssh.PublicKeys
		var err error

		// Use private key content if provided
		if in.cfg.auth.sshKey.privateKey != "" {
			publicKeys, err = ssh.NewPublicKeys("git", []byte(in.cfg.auth.sshKey.privateKey), in.cfg.auth.sshKey.passphrase)
			if err != nil {
				return nil, fmt.Errorf("failed to create SSH public keys from content: %w", err)
			}
		} else if in.cfg.auth.sshKey.privateKeyPath != "" {
			// Use private key file if provided
			publicKeys, err = ssh.NewPublicKeysFromFile("git", in.cfg.auth.sshKey.privateKeyPath, in.cfg.auth.sshKey.passphrase)
			if err != nil {
				return nil, fmt.Errorf("failed to create SSH public keys from file: %w", err)
			}
		} else {
			return nil, errors.New("SSH key authentication requires either private_key or private_key_path")
		}

		return publicKeys, nil
	}

	// No authentication configured
	return nil, nil
}
