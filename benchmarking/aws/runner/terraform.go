// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// Terraform shells out to the terraform CLI in a specific stack directory.
type Terraform struct {
	Dir         string // path under benchmarking/aws/terraform/stacks/<name> or shared
	BackendFile string // absolute path to backend.hcl
	StateKey    string // unique state key for this stack
}

// Init runs `terraform init` with the shared backend config.
func (t *Terraform) Init() error {
	args := []string{
		"-chdir=" + t.Dir,
		"init",
		"-input=false",
		"-backend-config=" + t.BackendFile,
		"-backend-config=key=" + t.StateKey + "/terraform.tfstate",
	}
	return run("terraform", args, nil)
}

// Apply applies the stack with the given variables. Each entry in vars becomes
// one -var "<k>=<v>" flag. Complex values must already be HCL-encoded strings.
func (t *Terraform) Apply(vars map[string]string) error {
	args := []string{
		"-chdir=" + t.Dir,
		"apply",
		"-input=false",
		"-auto-approve",
	}
	for k, v := range vars {
		args = append(args, "-var", fmt.Sprintf("%s=%s", k, v))
	}
	return run("terraform", args, nil)
}

// Destroy tears down the stack.
func (t *Terraform) Destroy(vars map[string]string) error {
	args := []string{
		"-chdir=" + t.Dir,
		"destroy",
		"-input=false",
		"-auto-approve",
	}
	for k, v := range vars {
		args = append(args, "-var", fmt.Sprintf("%s=%s", k, v))
	}
	return run("terraform", args, nil)
}

// Plan runs `terraform plan` and returns nil if successful.
func (t *Terraform) Plan(vars map[string]string) error {
	args := []string{
		"-chdir=" + t.Dir,
		"plan",
		"-input=false",
	}
	for k, v := range vars {
		args = append(args, "-var", fmt.Sprintf("%s=%s", k, v))
	}
	return run("terraform", args, nil)
}

// Outputs reads `terraform output -json` into a map[string]string.
// Non-string outputs are JSON-encoded into the value.
func (t *Terraform) Outputs() (map[string]string, error) {
	var buf bytes.Buffer
	args := []string{"-chdir=" + t.Dir, "output", "-json"}
	if err := run("terraform", args, &buf); err != nil {
		return nil, err
	}
	var raw map[string]struct {
		Sensitive bool            `json:"sensitive"`
		Type      json.RawMessage `json:"type"`
		Value     json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		var s string
		if err := json.Unmarshal(v.Value, &s); err == nil {
			out[k] = s
		} else {
			out[k] = string(v.Value)
		}
	}
	return out, nil
}

func run(name string, args []string, stdout *bytes.Buffer) error {
	cmd := exec.Command(name, args...)
	if stdout != nil {
		cmd.Stdout = stdout
	} else {
		cmd.Stdout = os.Stdout
	}
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// StackDir returns the path to a stack relative to the repo root.
func StackDir(repoRoot, stack string) string {
	return filepath.Join(repoRoot, "benchmarking", "aws", "terraform", "stacks", stack)
}

// SharedDir returns the path to the shared stack relative to the repo root.
func SharedDir(repoRoot string) string {
	return filepath.Join(repoRoot, "benchmarking", "aws", "terraform", "shared")
}
