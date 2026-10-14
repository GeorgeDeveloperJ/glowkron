package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/GeorgeDeveloperJ/glowkron/internal/config"
)

func TestResolvePaths_CustomHome(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "glowkron-config-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	paths, err := config.ResolvePaths(tempDir)
	if err != nil {
		t.Fatalf("unexpected error resolving paths: %v", err)
	}

	if paths.HomeDir != tempDir {
		t.Errorf("expected HomeDir %q, got %q", tempDir, paths.HomeDir)
	}
	if paths.DBFile != filepath.Join(tempDir, "glowkron.db") {
		t.Errorf("expected DBFile in tempDir, got %q", paths.DBFile)
	}
	if paths.WorkflowsDir != filepath.Join(tempDir, "workflows") {
		t.Errorf("expected WorkflowsDir in tempDir, got %q", paths.WorkflowsDir)
	}
	if paths.LogsDir != filepath.Join(tempDir, "logs") {
		t.Errorf("expected LogsDir in tempDir, got %q", paths.LogsDir)
	}

	// Verify EnsureDirs creates the subdirectories
	if err := paths.EnsureDirs(); err != nil {
		t.Fatalf("EnsureDirs failed: %v", err)
	}

	for _, dir := range []string{paths.HomeDir, paths.WorkflowsDir, paths.LogsDir} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Errorf("directory %q was not created: %v", dir, err)
		} else if !info.IsDir() {
			t.Errorf("path %q is not a directory", dir)
		}
	}
}

func TestResolvePaths_Default(t *testing.T) {
	paths, err := config.ResolvePaths("")
	if err != nil {
		t.Fatalf("failed to resolve default paths: %v", err)
	}
	if paths.HomeDir == "" || paths.DBFile == "" || paths.WorkflowsDir == "" || paths.LogsDir == "" {
		t.Errorf("paths fields should not be empty: %+v", paths)
	}
}
