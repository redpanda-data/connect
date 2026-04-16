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

package llmfix

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// agentStatus tracks the outcome of a fix agent dispatch.
type agentStatus struct {
	PkgPath string    `json:"pkg_path"`
	Status  string    `json:"status"`
	Error   string    `json:"error,omitempty"`
	Tag     string    `json:"tag"`
	Updated time.Time `json:"updated"`
}

const (
	statusDispatched = "dispatched"
	statusCompleted  = "completed"
	statusFailed     = "failed"
)

// NewTag generates a unique tag for a fix attempt. The tag is used as the
// worktree directory name, git branch name, and log file prefix.
func NewTag(slug string) string {
	return fmt.Sprintf("fix-%s-%s", slug, time.Now().Format("20060102150405"))
}

// Manager manages concurrent fix agents and serializes cherry-picks.
// All agent progress is logged to a shared agents.log file with per-agent prefixes.
type Manager struct {
	outputDir string
	baseSHA   string
	logFile   *os.File
	log       *log.Logger

	mu       sync.Mutex
	eg       *errgroup.Group
	statuses map[string]agentStatus
}

// NewManager creates a Manager that dispatches fix agents into outputDir.
// baseSHA pins worktrees so concurrent cherry-picks don't pollute them.
func NewManager(outputDir, baseSHA string, maxParallel int) (*Manager, error) {
	fixDir := filepath.Join(outputDir, "fix")
	if err := os.MkdirAll(fixDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating fix dir: %w", err)
	}

	logFile, err := os.OpenFile(filepath.Join(fixDir, "agents.log"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("creating agents log: %w", err)
	}

	eg := new(errgroup.Group)
	eg.SetLimit(maxParallel)

	l := log.New(logFile, "[manager] ", log.LstdFlags)
	l.Printf("initialized: baseSHA=%s, maxParallel=%d, outputDir=%s", baseSHA[:8], maxParallel, outputDir)

	m := &Manager{
		outputDir: outputDir,
		baseSHA:   baseSHA,
		logFile:   logFile,
		log:       l,
		eg:        eg,
		statuses:  make(map[string]agentStatus),
	}
	m.loadStatuses()

	return m, nil
}

// Close closes the shared agents log file.
func (m *Manager) Close() error {
	return m.logFile.Close()
}

func (m *Manager) statusPath() string {
	return filepath.Join(m.outputDir, "fix", "agents-status.json")
}

func (m *Manager) loadStatuses() {
	data, err := os.ReadFile(m.statusPath())
	if err != nil {
		return
	}
	if err := json.Unmarshal(data, &m.statuses); err != nil {
		m.log.Printf("warning: ignoring corrupt status file: %v", err)
	}
}

func (m *Manager) flushStatuses() {
	data, err := json.MarshalIndent(m.statuses, "", "  ")
	if err != nil {
		m.log.Printf("warning: failed to marshal status: %v", err)
		return
	}
	if err := os.WriteFile(m.statusPath(), data, 0o644); err != nil {
		m.log.Printf("warning: failed to write status file: %v", err)
	}
}

func (m *Manager) setAgentStatus(slug string, s agentStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s.Updated = time.Now()
	m.statuses[slug] = s
	m.flushStatuses()
}

// PendingSlugs returns slug to pkgPath for agents that didn't complete successfully.
func (m *Manager) PendingSlugs() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	pending := make(map[string]string)
	for slug, s := range m.statuses {
		if s.Status != statusCompleted {
			pending[slug] = s.PkgPath
		}
	}
	return pending
}

// Dispatch starts a fix pipeline for the given request in the background.
// The slug is used for tag/branch/file naming.
func (m *Manager) Dispatch(slug string, req FixRequest) {
	tag := NewTag(slug)

	// log.Logger is goroutine-safe; each Write call is a single syscall.
	l := log.New(m.logFile, "["+slug+"] ", log.LstdFlags)

	m.eg.Go(func() error {
		start := time.Now()
		l.Printf("starting fix pipeline")

		m.setAgentStatus(slug, agentStatus{
			PkgPath: req.PkgPath,
			Status:  statusDispatched,
			Tag:     tag,
		})

		err := m.runOperator(tag, req, l)

		status := statusCompleted
		var errMsg string
		if err != nil {
			status = statusFailed
			errMsg = err.Error()
			l.Printf("pipeline failed after %s: %v", time.Since(start).Truncate(time.Second), err)
		} else {
			l.Printf("pipeline completed in %s", time.Since(start).Truncate(time.Second))
		}

		m.setAgentStatus(slug, agentStatus{
			PkgPath: req.PkgPath,
			Status:  status,
			Error:   errMsg,
			Tag:     tag,
		})
		return nil
	})
}

func (m *Manager) runOperator(tag string, req FixRequest, l *log.Logger) error {
	dir, err := CreateWorktree(tag, m.baseSHA)
	if err != nil {
		return fmt.Errorf("creating worktree: %w", err)
	}
	l.Printf("worktree created: %s", dir)
	defer CleanupWorktree(dir, tag)

	req.Tag = tag
	req.OutputDir = m.outputDir
	req.WorktreeDir = dir

	op := NewOperator(req, l)
	if err := op.Run(); err != nil {
		return err
	}

	commits, err := m.cherryPickCommits(dir, m.baseSHA)
	if err != nil {
		return fmt.Errorf("cherry-pick: %w", err)
	}
	for _, c := range commits {
		l.Printf("cherry-picked: %s", c)
	}
	l.Printf("%d commit(s) applied", len(commits))

	return nil
}

// Recovery holds the result of recovering a single worktree.
type Recovery struct {
	Name    string
	Commits []string
	Err     error
}

// RecoverWorktrees cherry-picks commits from leftover worktrees onto HEAD.
func (m *Manager) RecoverWorktrees() []Recovery {
	entries, err := os.ReadDir(worktreeBase)
	if err != nil {
		return nil
	}

	var dirs []os.DirEntry
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e)
		}
	}
	if len(dirs) == 0 {
		return nil
	}

	m.log.Printf("recovering %d leftover worktree(s)", len(dirs))
	var results []Recovery
	for _, e := range dirs {
		r := m.recoverWorktreeDir(e.Name())
		if r.Err != nil {
			m.log.Printf("recover %s: %v", r.Name, r.Err)
		} else if len(r.Commits) > 0 {
			m.log.Printf("recover %s: %d commit(s) applied", r.Name, len(r.Commits))
		} else {
			m.log.Printf("recover %s: no commits to apply", r.Name)
		}
		results = append(results, r)
	}
	return results
}

// IsFixing reports whether a fix agent is currently running for the given slug.
func (m *Manager) IsFixing(slug string) bool {
	m.mu.Lock()
	s, ok := m.statuses[slug]
	m.mu.Unlock()
	return ok && s.Status == statusDispatched
}

// Wait blocks until all dispatched fix agents complete.
func (m *Manager) Wait() {
	m.log.Printf("waiting for fix agents to finish")
	_ = m.eg.Wait()
	m.log.Printf("all fix agents finished")
}

func (m *Manager) recoverWorktreeDir(name string) Recovery {
	r := Recovery{Name: name}
	dir := filepath.Join(worktreeBase, name)

	branchOut, err := exec.Command("git", "-C", dir, "rev-parse", "--abbrev-ref", "HEAD").Output()
	if err != nil {
		_ = exec.Command("git", "worktree", "remove", "--force", dir).Run()
		r.Err = fmt.Errorf("resolving branch: %w", err)
		return r
	}
	branch := strings.TrimSpace(string(branchOut))
	defer CleanupWorktree(dir, branch)

	baseOut, err := exec.Command("git", "merge-base", "HEAD", branch).Output()
	if err != nil {
		r.Err = fmt.Errorf("finding merge base: %w", err)
		return r
	}
	baseSHA := strings.TrimSpace(string(baseOut))

	r.Commits, r.Err = m.cherryPickCommits(dir, baseSHA)
	return r
}

func (m *Manager) cherryPickCommits(dir, baseSHA string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return CherryPickCommits(dir, baseSHA)
}
