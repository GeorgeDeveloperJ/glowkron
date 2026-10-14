package workflow

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const sampleDailyBackupYAML = `version: "1"
name: "daily-database-backup"
description: "Dumps SQLite/Postgres and uploads encrypted archive"
schedule: "0 2 * * *"
timeout: 1800
max_retries: 2
retry_delay: 15s

notifications:
  on_failure: true
  on_success: false
  webhook_url: "https://chat.googleapis.com/v1/spaces/..."

steps:
  - id: dump
    name: "Export Database"
    command: "pg_dump"
    args: ["-U", "postgres", "-f", "/tmp/backup.sql", "mydb"]
    timeout: 300s

  - id: compress
    name: "Compress Archive"
    command: "gzip"
    args: ["-f", "/tmp/backup.sql"]
    depends_on: ["dump"]

  - id: upload
    name: "Upload to S3"
    command: "aws"
    args: ["s3", "cp", "/tmp/backup.sql.gz", "s3://my-bucket/backups/"]
    depends_on: ["compress"]
`

func TestParseValidWorkflow(t *testing.T) {
	wf, err := ParseBytes([]byte(sampleDailyBackupYAML))
	if err != nil {
		t.Fatalf("unexpected error parsing valid workflow: %v", err)
	}

	if wf.Version != "1" {
		t.Errorf("expected version '1', got %q", wf.Version)
	}
	if wf.Name != "daily-database-backup" {
		t.Errorf("expected name 'daily-database-backup', got %q", wf.Name)
	}
	if wf.Description != "Dumps SQLite/Postgres and uploads encrypted archive" {
		t.Errorf("unexpected description: %q", wf.Description)
	}
	if wf.Schedule != "0 2 * * *" {
		t.Errorf("expected schedule '0 2 * * *', got %q", wf.Schedule)
	}
	if wf.Timeout != 1800*time.Second {
		t.Errorf("expected timeout 1800s, got %v", wf.Timeout)
	}
	if wf.MaxRetries != 2 {
		t.Errorf("expected max_retries 2, got %d", wf.MaxRetries)
	}
	if wf.RetryDelay != 15*time.Second {
		t.Errorf("expected retry_delay 15s, got %v", wf.RetryDelay)
	}

	// Notifications
	if !wf.Notifications.OnFailure {
		t.Errorf("expected on_failure true")
	}
	if wf.Notifications.OnSuccess {
		t.Errorf("expected on_success false")
	}
	if wf.Notifications.WebhookURL != "https://chat.googleapis.com/v1/spaces/..." {
		t.Errorf("unexpected webhook_url: %q", wf.Notifications.WebhookURL)
	}

	// Steps
	if len(wf.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(wf.Steps))
	}

	// Step 0: dump
	s0 := wf.Steps[0]
	if s0.ID != "dump" || s0.Name != "Export Database" || s0.Command != "pg_dump" {
		t.Errorf("step 0 mismatch: %+v", s0)
	}
	if len(s0.Args) != 5 || s0.Args[0] != "-U" || s0.Args[4] != "mydb" {
		t.Errorf("step 0 args mismatch: %v", s0.Args)
	}
	if s0.Timeout != 300*time.Second {
		t.Errorf("expected step 0 timeout 300s, got %v", s0.Timeout)
	}
	if len(s0.DependsOn) != 0 {
		t.Errorf("step 0 should have no dependencies, got %v", s0.DependsOn)
	}

	// Step 1: compress
	s1 := wf.Steps[1]
	if s1.ID != "compress" || s1.Command != "gzip" {
		t.Errorf("step 1 mismatch: %+v", s1)
	}
	if len(s1.DependsOn) != 1 || s1.DependsOn[0] != "dump" {
		t.Errorf("step 1 dependencies mismatch: %v", s1.DependsOn)
	}

	// Step 2: upload
	s2 := wf.Steps[2]
	if s2.ID != "upload" || s2.Command != "aws" {
		t.Errorf("step 2 mismatch: %+v", s2)
	}
	if len(s2.DependsOn) != 1 || s2.DependsOn[0] != "compress" {
		t.Errorf("step 2 dependencies mismatch: %v", s2.DependsOn)
	}
}

func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name          string
		modifyYAML    func(string) string
		expectedField string
		expectedStep  string
		expectedErr   string
	}{
		{
			name: "empty name",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `name: "daily-database-backup"`, `name: ""`, 1)
			},
			expectedField: "name",
			expectedErr:   "name cannot be empty",
		},
		{
			name: "invalid name slug with spaces",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `name: "daily-database-backup"`, `name: "daily database backup"`, 1)
			},
			expectedField: "name",
			expectedErr:   "must be a valid identifier/slug",
		},
		{
			name: "duplicate step ID",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `id: compress`, `id: dump`, 1)
			},
			expectedField: "id",
			expectedStep:  "dump",
			expectedErr:   "duplicate step ID",
		},
		{
			name: "missing step command",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `command: "pg_dump"`, `command: ""`, 1)
			},
			expectedField: "command",
			expectedStep:  "dump",
			expectedErr:   "command cannot be empty",
		},
		{
			name: "unknown depends_on step ID",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `depends_on: ["dump"]`, `depends_on: ["non-existent-step"]`, 1)
			},
			expectedField: "depends_on",
			expectedStep:  "compress",
			expectedErr:   "depends on unknown step \"non-existent-step\"",
		},
		{
			name: "self-dependency",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `depends_on: ["dump"]`, `depends_on: ["compress"]`, 1)
			},
			expectedField: "depends_on",
			expectedStep:  "compress",
			expectedErr:   "cannot depend on itself",
		},
		{
			name: "empty steps list",
			modifyYAML: func(y string) string {
				idx := strings.Index(y, "steps:")
				return y[:idx] + "steps: []\n"
			},
			expectedField: "steps",
			expectedErr:   "must contain at least one step",
		},
		{
			name: "empty version",
			modifyYAML: func(y string) string {
				return strings.Replace(y, `version: "1"`, `version: ""`, 1)
			},
			expectedField: "version",
			expectedErr:   "version cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yamlContent := tt.modifyYAML(sampleDailyBackupYAML)
			_, err := ParseBytes([]byte(yamlContent))
			if err == nil {
				t.Fatalf("expected validation error for %s, got nil", tt.name)
			}

			var valErrs ValidationErrors
			if !errors.As(err, &valErrs) {
				t.Fatalf("expected error to be ValidationErrors, got: %T (%v)", err, err)
			}

			if tt.expectedField != "" && !valErrs.HasField(tt.expectedField) {
				t.Errorf("expected error field %q, errors were: %v", tt.expectedField, valErrs)
			}

			if tt.expectedStep != "" && !valErrs.HasStep(tt.expectedStep) {
				t.Errorf("expected error step %q, errors were: %v", tt.expectedStep, valErrs)
			}

			if tt.expectedErr != "" && !strings.Contains(err.Error(), tt.expectedErr) {
				t.Errorf("expected error message containing %q, got: %v", tt.expectedErr, err)
			}
		})
	}
}

func TestDurationParsing(t *testing.T) {
	tests := []struct {
		input       string
		expected    time.Duration
		expectError bool
	}{
		// Numeric seconds (as string or scalar)
		{"1800", 1800 * time.Second, false},
		{"15", 15 * time.Second, false},
		{"300", 300 * time.Second, false},
		{"0", 0, false},
		{"", 0, false},

		// Standard Go duration strings
		{"15s", 15 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"300s", 300 * time.Second, false},
		{"1h", 1 * time.Hour, false},
		{"1h30m", 90 * time.Minute, false},
		{"500ms", 500 * time.Millisecond, false},

		// Float seconds
		{"1.5", 1500 * time.Millisecond, false},

		// Negative durations (invalid)
		{"-10s", 0, true},
		{"-5", 0, true},

		// Non-duration garbage
		{"invalid", 0, true},
		{"15x", 0, true},
		{"1.2.3", 0, true},
	}

	for _, tt := range tests {
		t.Run("ParseDuration_"+tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error parsing duration %q, got %v", tt.input, got)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error parsing duration %q: %v", tt.input, err)
				}
				if got != tt.expected {
					t.Errorf("ParseDuration(%q) = %v, expected %v", tt.input, got, tt.expected)
				}
			}
		})
	}
}

func TestDurationInYAML(t *testing.T) {
	yamlSnippet := `
version: "1"
name: "dur-test"
timeout: 1800
retry_delay: 15s
steps:
  - id: s1
    name: "step one"
    command: "echo"
    timeout: 5m
`
	wf, err := ParseBytes([]byte(yamlSnippet))
	if err != nil {
		t.Fatalf("unexpected error parsing YAML: %v", err)
	}

	if wf.Timeout != 1800*time.Second {
		t.Errorf("expected timeout 1800s, got %v", wf.Timeout)
	}
	if wf.RetryDelay != 15*time.Second {
		t.Errorf("expected retry_delay 15s, got %v", wf.RetryDelay)
	}
	if wf.Steps[0].Timeout != 5*time.Minute {
		t.Errorf("expected step timeout 5m, got %v", wf.Steps[0].Timeout)
	}
}

func TestParseReader(t *testing.T) {
	wf, err := Parse(strings.NewReader(sampleDailyBackupYAML))
	if err != nil {
		t.Fatalf("unexpected error parsing from reader: %v", err)
	}
	if wf.Name != "daily-database-backup" {
		t.Errorf("unexpected workflow name: %q", wf.Name)
	}

	// Nil reader
	_, err = Parse(nil)
	if err == nil {
		t.Errorf("expected error for nil reader")
	}
}

func TestParseFile(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "daily-backup.yaml")

	if err := os.WriteFile(filePath, []byte(sampleDailyBackupYAML), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	wf, err := ParseFile(filePath)
	if err != nil {
		t.Fatalf("unexpected error parsing file: %v", err)
	}

	if wf.SourceFile != filePath {
		t.Errorf("expected SourceFile %q, got %q", filePath, wf.SourceFile)
	}
	if wf.Name != "daily-database-backup" {
		t.Errorf("expected name 'daily-database-backup', got %q", wf.Name)
	}

	// Non-existent file
	_, err = ParseFile(filepath.Join(tempDir, "missing.yaml"))
	if err == nil {
		t.Errorf("expected error for non-existent file")
	}
}

func TestParseBytes_Empty(t *testing.T) {
	_, err := ParseBytes([]byte(""))
	if err == nil {
		t.Errorf("expected error for empty byte slice")
	}

	_, err = ParseBytes([]byte("   \n\t  "))
	if err == nil {
		t.Errorf("expected error for whitespace byte slice")
	}
}

func TestParseBytes_MalformedYAML(t *testing.T) {
	malformed := `
version: "1"
name: [unterminated
`
	_, err := ParseBytes([]byte(malformed))
	if err == nil {
		t.Errorf("expected error for malformed YAML")
	}
}

func TestJSONRoundtrip(t *testing.T) {
	wf, err := ParseBytes([]byte(sampleDailyBackupYAML))
	if err != nil {
		t.Fatalf("failed to parse valid YAML: %v", err)
	}

	jsonData, err := json.Marshal(wf)
	if err != nil {
		t.Fatalf("failed to marshal workflow to JSON: %v", err)
	}

	var roundtrip WorkflowConfig
	if err := json.Unmarshal(jsonData, &roundtrip); err != nil {
		t.Fatalf("failed to unmarshal workflow from JSON: %v", err)
	}

	if roundtrip.Name != wf.Name {
		t.Errorf("JSON roundtrip name mismatch: got %q, want %q", roundtrip.Name, wf.Name)
	}
	if roundtrip.Timeout != wf.Timeout {
		t.Errorf("JSON roundtrip timeout mismatch: got %v, want %v", roundtrip.Timeout, wf.Timeout)
	}
	if roundtrip.RetryDelay != wf.RetryDelay {
		t.Errorf("JSON roundtrip retry_delay mismatch: got %v, want %v", roundtrip.RetryDelay, wf.RetryDelay)
	}
	if len(roundtrip.Steps) != len(wf.Steps) {
		t.Fatalf("JSON roundtrip steps length mismatch: got %d, want %d", len(roundtrip.Steps), len(wf.Steps))
	}
	if roundtrip.Steps[0].Timeout != wf.Steps[0].Timeout {
		t.Errorf("JSON roundtrip step 0 timeout mismatch: got %v, want %v", roundtrip.Steps[0].Timeout, wf.Steps[0].Timeout)
	}
}

func TestValidateNilWorkflow(t *testing.T) {
	err := Validate(nil)
	if err == nil {
		t.Errorf("expected error when validating nil workflow")
	}
}

func TestYAMLRoundtrip(t *testing.T) {
	wf, err := ParseBytes([]byte(sampleDailyBackupYAML))
	if err != nil {
		t.Fatalf("failed to parse valid YAML: %v", err)
	}

	marshaled, err := yaml.Marshal(wf)
	if err != nil {
		t.Fatalf("failed to marshal workflow to YAML: %v", err)
	}

	roundtrip, err := ParseBytes(marshaled)
	if err != nil {
		t.Fatalf("failed to parse marshaled YAML: %v", err)
	}

	if roundtrip.Name != wf.Name {
		t.Errorf("YAML roundtrip name mismatch: got %q, want %q", roundtrip.Name, wf.Name)
	}
	if roundtrip.Timeout != wf.Timeout {
		t.Errorf("YAML roundtrip timeout mismatch: got %v, want %v", roundtrip.Timeout, wf.Timeout)
	}
	if len(roundtrip.Steps) != len(wf.Steps) {
		t.Errorf("YAML roundtrip step count mismatch: got %d, want %d", len(roundtrip.Steps), len(wf.Steps))
	}
}
