package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GeorgeDeveloperJ/glowkron/internal/dag"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

func TestInit_Clean(t *testing.T) {
	tempDir := t.TempDir()

	cliCtx := &CLIContext{}
	cmd := NewRootCmd(cliCtx)

	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetErr(&buf)
	cmd.SetArgs([]string{"--home", tempDir, "init", "--with-config"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("expected init to succeed, got: %v", err)
	}

	// Verify directory structures
	workflowsDir := filepath.Join(tempDir, "workflows")
	if info, err := os.Stat(workflowsDir); err != nil || !info.IsDir() {
		t.Fatalf("expected workflows directory to exist at %s", workflowsDir)
	}

	logsDir := filepath.Join(tempDir, "logs")
	if info, err := os.Stat(logsDir); err != nil || !info.IsDir() {
		t.Fatalf("expected logs directory to exist at %s", logsDir)
	}

	// Verify sample workflow was created
	sampleFile := filepath.Join(workflowsDir, "sample-pipeline.yaml")
	if _, err := os.Stat(sampleFile); err != nil {
		t.Fatalf("expected sample workflow to exist at %s, got err: %v", sampleFile, err)
	}

	// Verify config.yaml was created with documented defaults
	configFile := filepath.Join(tempDir, "config.yaml")
	configBytes, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("expected config.yaml to exist at %s, got err: %v", configFile, err)
	}
	configStr := string(configBytes)
	for _, expected := range []string{"port: 8080", "workflows_dir:", "db_path:", "log_level: info"} {
		if !strings.Contains(configStr, expected) {
			t.Errorf("expected config.yaml to contain %q, content:\n%s", expected, configStr)
		}
	}

	// Verify banner and next steps in output
	output := buf.String()
	for _, expected := range []string{
		"✨ GlowKron Initialized Successfully",
		"glowkron list",
		"glowkron run sample-pipeline --follow",
		"glowkron daemon (visit http://localhost:8080)",
	} {
		if !strings.Contains(output, expected) {
			t.Errorf("expected output to contain %q, got:\n%s", expected, output)
		}
	}
}

func TestInit_PreserveWithoutForce(t *testing.T) {
	tempDir := t.TempDir()

	// 1. First run to initialize
	cmd := NewRootCmd(&CLIContext{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--home", tempDir, "init", "--with-config"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial init failed: %v", err)
	}

	sampleFile := filepath.Join(tempDir, "workflows", "sample-pipeline.yaml")
	configFile := filepath.Join(tempDir, "config.yaml")

	// 2. Modify sample workflow and config
	customWorkflow := []byte("version: \"1.0\"\nname: custom-pipeline\nsteps:\n  - id: step-custom\n    command: echo\n")
	if err := os.WriteFile(sampleFile, customWorkflow, 0644); err != nil {
		t.Fatalf("failed to modify sample workflow: %v", err)
	}
	customConfig := []byte("port: 9999\nlog_level: debug\n")
	if err := os.WriteFile(configFile, customConfig, 0644); err != nil {
		t.Fatalf("failed to modify config.yaml: %v", err)
	}

	// 3. Second run without --force
	var buf bytes.Buffer
	cmd2 := NewRootCmd(&CLIContext{})
	cmd2.SetOut(&buf)
	cmd2.SetErr(&buf)
	cmd2.SetArgs([]string{"--home", tempDir, "init", "--with-config"})
	if err := cmd2.Execute(); err != nil {
		t.Fatalf("second init failed: %v", err)
	}

	// Verify contents preserved
	wfBytes, err := os.ReadFile(sampleFile)
	if err != nil {
		t.Fatalf("failed to read workflow file: %v", err)
	}
	if string(wfBytes) != string(customWorkflow) {
		t.Errorf("expected workflow to be preserved, got:\n%s", string(wfBytes))
	}

	cfgBytes, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}
	if string(cfgBytes) != string(customConfig) {
		t.Errorf("expected config to be preserved, got:\n%s", string(cfgBytes))
	}

	// Verify output notices
	outStr := buf.String()
	if !strings.Contains(outStr, "Notice: starter workflow already exists") {
		t.Errorf("expected notice about existing starter workflow, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "Notice: configuration file already exists") {
		t.Errorf("expected notice about existing configuration file, got:\n%s", outStr)
	}
}

func TestInit_OverwriteWithForce(t *testing.T) {
	tempDir := t.TempDir()

	// 1. First run to initialize
	cmd := NewRootCmd(&CLIContext{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--home", tempDir, "init", "--with-config"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial init failed: %v", err)
	}

	sampleFile := filepath.Join(tempDir, "workflows", "sample-pipeline.yaml")
	configFile := filepath.Join(tempDir, "config.yaml")

	// 2. Modify files with custom content
	customWorkflow := []byte("version: \"1.0\"\nname: custom-pipeline\nsteps:\n  - id: step-custom\n    command: echo\n")
	if err := os.WriteFile(sampleFile, customWorkflow, 0644); err != nil {
		t.Fatalf("failed to write custom workflow: %v", err)
	}
	customConfig := []byte("port: 9999\nlog_level: debug\n")
	if err := os.WriteFile(configFile, customConfig, 0644); err != nil {
		t.Fatalf("failed to write custom config: %v", err)
	}

	// 3. Re-run with --force (using short flag -f)
	var buf bytes.Buffer
	cmd2 := NewRootCmd(&CLIContext{})
	cmd2.SetOut(&buf)
	cmd2.SetErr(&buf)
	cmd2.SetArgs([]string{"--home", tempDir, "init", "-f", "--with-config"})
	if err := cmd2.Execute(); err != nil {
		t.Fatalf("init with -f failed: %v", err)
	}

	// Verify starter workflow was overwritten
	wfBytes, err := os.ReadFile(sampleFile)
	if err != nil {
		t.Fatalf("failed to read workflow file: %v", err)
	}
	if strings.Contains(string(wfBytes), "custom-pipeline") {
		t.Errorf("expected starter workflow to be overwritten, got:\n%s", string(wfBytes))
	}
	if !strings.Contains(string(wfBytes), "name: sample-pipeline") {
		t.Errorf("expected sample-pipeline in overwritten file, got:\n%s", string(wfBytes))
	}

	// Verify config.yaml was overwritten
	cfgBytes, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}
	if strings.Contains(string(cfgBytes), "port: 9999") {
		t.Errorf("expected config to be overwritten, got:\n%s", string(cfgBytes))
	}
	if !strings.Contains(string(cfgBytes), "port: 8080") {
		t.Errorf("expected default port: 8080, got:\n%s", string(cfgBytes))
	}

	// Verify output notices
	outStr := buf.String()
	if !strings.Contains(outStr, "Overwrote starter workflow") {
		t.Errorf("expected 'Overwrote starter workflow' in output, got:\n%s", outStr)
	}
	if !strings.Contains(outStr, "Overwrote configuration file") {
		t.Errorf("expected 'Overwrote configuration file' in output, got:\n%s", outStr)
	}
}

func TestInit_GeneratedWorkflowParsesAndValidates(t *testing.T) {
	tempDir := t.TempDir()

	cmd := NewRootCmd(&CLIContext{})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--home", tempDir, "init"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("init failed: %v", err)
	}

	samplePath := filepath.Join(tempDir, "workflows", "sample-pipeline.yaml")
	wf, err := workflow.ParseFile(samplePath)
	if err != nil {
		t.Fatalf("workflow.ParseFile failed: %v", err)
	}

	// Validate workflow metadata
	if wf.Name != "sample-pipeline" {
		t.Errorf("expected workflow name 'sample-pipeline', got %q", wf.Name)
	}
	if len(wf.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(wf.Steps))
	}

	// Validate step 1: Fetch Source Data
	s1 := wf.Steps[0]
	if s1.ID != "step-1" || s1.Name != "Fetch Source Data" || s1.Command != "echo" {
		t.Errorf("unexpected step 1: %+v", s1)
	}
	if len(s1.Args) != 1 || s1.Args[0] != "Fetched latest dataset" {
		t.Errorf("unexpected step 1 args: %+v", s1.Args)
	}
	if len(s1.DependsOn) != 0 {
		t.Errorf("step 1 should have no dependencies, got %+v", s1.DependsOn)
	}

	// Validate step 2: Transform & Process
	s2 := wf.Steps[1]
	if s2.ID != "step-2" || s2.Name != "Transform & Process" || s2.Command != "echo" {
		t.Errorf("unexpected step 2: %+v", s2)
	}
	if len(s2.Args) != 1 || s2.Args[0] != "Data transformed successfully" {
		t.Errorf("unexpected step 2 args: %+v", s2.Args)
	}
	if len(s2.DependsOn) != 1 || s2.DependsOn[0] != "step-1" {
		t.Errorf("step 2 should depend on ['step-1'], got %+v", s2.DependsOn)
	}

	// Validate step 3: Publish Summary
	s3 := wf.Steps[2]
	if s3.ID != "step-3" || s3.Name != "Publish Summary" || s3.Command != "echo" {
		t.Errorf("unexpected step 3: %+v", s3)
	}
	if len(s3.Args) != 1 || s3.Args[0] != "Pipeline execution complete" {
		t.Errorf("unexpected step 3 args: %+v", s3.Args)
	}
	if len(s3.DependsOn) != 1 || s3.DependsOn[0] != "step-2" {
		t.Errorf("step 3 should depend on ['step-2'], got %+v", s3.DependsOn)
	}

	// Verify DAG resolution
	plan, _, err := dag.BuildAndResolve(wf.Steps)
	if err != nil {
		t.Fatalf("expected DAG to resolve without cycle, got: %v", err)
	}
	if plan.StageCount() != 3 {
		t.Errorf("expected 3 stages in sequential DAG, got %d", plan.StageCount())
	}
}
