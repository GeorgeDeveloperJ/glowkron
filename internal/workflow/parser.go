package workflow

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// Parse reads workflow YAML from an io.Reader, deserializes it into WorkflowConfig,
// and validates the schema.
func Parse(r io.Reader) (*WorkflowConfig, error) {
	if r == nil {
		return nil, errors.New("reader cannot be nil")
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read workflow data: %w", err)
	}
	return ParseBytes(data)
}

// ParseBytes deserializes workflow YAML bytes into WorkflowConfig and validates it.
func ParseBytes(data []byte) (*WorkflowConfig, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("workflow content cannot be empty")
	}

	var wf WorkflowConfig
	if err := yaml.Unmarshal(trimmed, &wf); err != nil {
		return nil, fmt.Errorf("failed to parse workflow YAML: %w", err)
	}

	if err := Validate(&wf); err != nil {
		return nil, err
	}

	return &wf, nil
}

// ParseBytesUnvalidated deserializes workflow YAML bytes into WorkflowConfig without calling Validate.
func ParseBytesUnvalidated(data []byte) (*WorkflowConfig, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("workflow content cannot be empty")
	}

	var wf WorkflowConfig
	if err := yaml.Unmarshal(trimmed, &wf); err != nil {
		return nil, fmt.Errorf("failed to parse workflow YAML: %w", err)
	}

	return &wf, nil
}

// ParseFile reads a workflow YAML file from path, deserializes it into WorkflowConfig,
// and validates it. The SourceFile field on WorkflowConfig is automatically set to the path.
func ParseFile(path string) (*WorkflowConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read workflow file %q: %w", path, err)
	}

	wf, err := ParseBytes(data)
	if err != nil {
		return nil, err
	}

	wf.SourceFile = path
	return wf, nil
}
