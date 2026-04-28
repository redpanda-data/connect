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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const worktreeBase = ".integration/worktree"

// CreateWorktree creates a git worktree pinned to baseSHA so that concurrent
// cherry-picks advancing HEAD don't pollute new worktrees.
func CreateWorktree(tag, baseSHA string) (string, error) {
	path := filepath.Join(worktreeBase, tag)

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("creating worktree parent dir: %w", err)
	}

	out, err := exec.Command("git", "worktree", "add", "-b", tag, path, baseSHA).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git worktree add: %w\n%s", err, out)
	}

	return path, nil
}

// CleanupWorktree removes the worktree directory and its branch. Best-effort.
func CleanupWorktree(dir, branch string) {
	_ = exec.Command("git", "worktree", "remove", "--force", dir).Run()
	_ = exec.Command("git", "branch", "-D", branch).Run()
}

// CherryPickCommits cherry-picks all commits from dir that are ahead of baseSHA
// onto the current branch.
func CherryPickCommits(dir, baseSHA string) ([]string, error) {
	out, err := exec.Command("git", "-C", dir, "log", "--format=%H %s", "--reverse", baseSHA+"..HEAD").Output()
	if err != nil {
		return nil, fmt.Errorf("listing worktree commits: %w", err)
	}

	raw := strings.TrimSpace(string(out))
	if raw == "" {
		return nil, nil
	}

	lines := strings.Split(raw, "\n")
	var picked []string
	for _, line := range lines {
		sha, _, _ := strings.Cut(line, " ")
		cpOut, err := exec.Command("git", "cherry-pick", sha).CombinedOutput()
		if err != nil {
			_ = exec.Command("git", "cherry-pick", "--abort").Run()
			return picked, fmt.Errorf("cherry-pick %s: %w\n%s", sha[:8], err, cpOut)
		}
		picked = append(picked, line)
	}

	return picked, nil
}
