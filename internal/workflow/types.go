package workflow

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a custom wrapper around time.Duration that supports unmarshaling
// from both standard Go duration strings (e.g. "15s", "5m", "300s") and numeric
// seconds represented as integers or floats (e.g. 1800, "1800", 1.5).
type Duration time.Duration

// Duration returns the underlying time.Duration value.
func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

// String returns the string representation of the duration.
func (d Duration) String() string {
	return time.Duration(d).String()
}

// ParseDuration parses duration from string representations or numeric seconds.
// Supported formats:
//   - Go duration strings: "15s", "5m", "300s", "1h30m", "500ms"
//   - Numeric seconds (integer or float): "1800", "15", "1.5"
//   - Empty string returns 0, nil.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	// 1. Try integer seconds (e.g. "1800", "15", "300")
	if sec, err := strconv.ParseInt(s, 10, 64); err == nil {
		if sec < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %d", sec)
		}
		return time.Duration(sec) * time.Second, nil
	}

	// 2. Try float seconds (e.g. "1.5")
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %g", f)
		}
		return time.Duration(f * float64(time.Second)), nil
	}

	// 3. Try standard Go duration string (e.g. "15s", "5m", "300s")
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration format %q: %w", s, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("duration cannot be negative: %v", d)
	}

	return d, nil
}

// ParseDurationValue extracts a time.Duration from supported primitive types
// (time.Duration, Duration, string, int, int64, float64).
func ParseDurationValue(v any) (time.Duration, error) {
	switch val := v.(type) {
	case nil:
		return 0, nil
	case time.Duration:
		if val < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %v", val)
		}
		return val, nil
	case Duration:
		d := val.Duration()
		if d < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %v", d)
		}
		return d, nil
	case int:
		if val < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %d", val)
		}
		return time.Duration(val) * time.Second, nil
	case int64:
		if val < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %d", val)
		}
		return time.Duration(val) * time.Second, nil
	case float64:
		if val < 0 {
			return 0, fmt.Errorf("duration cannot be negative: %g", val)
		}
		return time.Duration(val * float64(time.Second)), nil
	case string:
		return ParseDuration(val)
	default:
		return 0, fmt.Errorf("unsupported duration type %T (%v)", v, v)
	}
}

// UnmarshalYAML implements yaml.Unmarshaler for Duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Value == "" || value.Tag == "!!null" || value.Value == "null" {
		*d = 0
		return nil
	}
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("cannot unmarshal YAML kind %d into Duration", value.Kind)
	}
	dur, err := ParseDuration(value.Value)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// MarshalYAML implements yaml.Marshaler for Duration.
func (d Duration) MarshalYAML() (any, error) {
	if d == 0 {
		return "0s", nil
	}
	return time.Duration(d).String(), nil
}

// UnmarshalJSON implements json.Unmarshaler for Duration.
func (d *Duration) UnmarshalJSON(data []byte) error {
	s := strings.TrimSpace(string(data))
	if s == "null" || s == `""` || s == "" {
		*d = 0
		return nil
	}
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		var strVal string
		if err := json.Unmarshal(data, &strVal); err != nil {
			return err
		}
		dur, err := ParseDuration(strVal)
		if err != nil {
			return err
		}
		*d = Duration(dur)
		return nil
	}
	var num float64
	if err := json.Unmarshal(data, &num); err != nil {
		return err
	}
	dur, err := ParseDurationValue(num)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

// MarshalJSON implements json.Marshaler for Duration.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// NotificationConfig defines alert routing for workflow execution events.
type NotificationConfig struct {
	OnFailure  bool   `yaml:"on_failure" json:"on_failure"`
	OnSuccess  bool   `yaml:"on_success" json:"on_success"`
	WebhookURL string `yaml:"webhook_url,omitempty" json:"webhook_url,omitempty"`
}

// StepConfig defines an individual execution task within a workflow DAG.
type StepConfig struct {
	ID         string            `yaml:"id" json:"id"`
	Name       string            `yaml:"name" json:"name"`
	Command    string            `yaml:"command" json:"command"`
	Args       []string          `yaml:"args,omitempty" json:"args,omitempty"`
	WorkingDir string            `yaml:"working_dir,omitempty" json:"working_dir,omitempty"`
	Env        map[string]string `yaml:"env,omitempty" json:"env,omitempty"`
	DependsOn  []string          `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Timeout    time.Duration     `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// UnmarshalYAML implements custom YAML unmarshaling for StepConfig to support flexible duration formats.
func (s *StepConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawStep struct {
		ID         string            `yaml:"id"`
		Name       string            `yaml:"name"`
		Command    string            `yaml:"command"`
		Args       []string          `yaml:"args,omitempty"`
		WorkingDir string            `yaml:"working_dir,omitempty"`
		Env        map[string]string `yaml:"env,omitempty"`
		DependsOn  []string          `yaml:"depends_on,omitempty"`
		Timeout    Duration          `yaml:"timeout,omitempty"`
	}

	var raw rawStep
	if err := value.Decode(&raw); err != nil {
		return err
	}

	s.ID = raw.ID
	s.Name = raw.Name
	s.Command = raw.Command
	s.Args = raw.Args
	s.WorkingDir = raw.WorkingDir
	s.Env = raw.Env
	s.DependsOn = raw.DependsOn
	s.Timeout = raw.Timeout.Duration()
	return nil
}

// MarshalYAML implements custom YAML marshaling for StepConfig.
func (s StepConfig) MarshalYAML() (any, error) {
	type rawStep struct {
		ID         string            `yaml:"id"`
		Name       string            `yaml:"name"`
		Command    string            `yaml:"command"`
		Args       []string          `yaml:"args,omitempty"`
		WorkingDir string            `yaml:"working_dir,omitempty"`
		Env        map[string]string `yaml:"env,omitempty"`
		DependsOn  []string          `yaml:"depends_on,omitempty"`
		Timeout    string            `yaml:"timeout,omitempty"`
	}

	raw := rawStep{
		ID:         s.ID,
		Name:       s.Name,
		Command:    s.Command,
		Args:       s.Args,
		WorkingDir: s.WorkingDir,
		Env:        s.Env,
		DependsOn:  s.DependsOn,
	}
	if s.Timeout > 0 {
		raw.Timeout = s.Timeout.String()
	}
	return raw, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for StepConfig.
func (s *StepConfig) UnmarshalJSON(data []byte) error {
	type rawStep struct {
		ID         string            `json:"id"`
		Name       string            `json:"name"`
		Command    string            `json:"command"`
		Args       []string          `json:"args,omitempty"`
		WorkingDir string            `json:"working_dir,omitempty"`
		Env        map[string]string `json:"env,omitempty"`
		DependsOn  []string          `json:"depends_on,omitempty"`
		Timeout    Duration          `json:"timeout,omitempty"`
	}

	var raw rawStep
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	s.ID = raw.ID
	s.Name = raw.Name
	s.Command = raw.Command
	s.Args = raw.Args
	s.WorkingDir = raw.WorkingDir
	s.Env = raw.Env
	s.DependsOn = raw.DependsOn
	s.Timeout = raw.Timeout.Duration()
	return nil
}

// MarshalJSON implements custom JSON marshaling for StepConfig.
func (s StepConfig) MarshalJSON() ([]byte, error) {
	type rawStep struct {
		ID         string            `json:"id"`
		Name       string            `json:"name"`
		Command    string            `json:"command"`
		Args       []string          `json:"args,omitempty"`
		WorkingDir string            `json:"working_dir,omitempty"`
		Env        map[string]string `json:"env,omitempty"`
		DependsOn  []string          `json:"depends_on,omitempty"`
		Timeout    string            `json:"timeout,omitempty"`
	}

	raw := rawStep{
		ID:         s.ID,
		Name:       s.Name,
		Command:    s.Command,
		Args:       s.Args,
		WorkingDir: s.WorkingDir,
		Env:        s.Env,
		DependsOn:  s.DependsOn,
	}
	if s.Timeout > 0 {
		raw.Timeout = s.Timeout.String()
	}
	return json.Marshal(raw)
}

// WorkflowConfig defines the complete specification of a declarative workflow job.
type WorkflowConfig struct {
	Version          string             `yaml:"version" json:"version"`
	Name             string             `yaml:"name" json:"name"`
	Description      string             `yaml:"description,omitempty" json:"description,omitempty"`
	Schedule         string             `yaml:"schedule,omitempty" json:"schedule,omitempty"`
	Timeout          time.Duration      `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	MaxRetries       int                `yaml:"max_retries,omitempty" json:"max_retries,omitempty"`
	RetryDelay       time.Duration      `yaml:"retry_delay,omitempty" json:"retry_delay,omitempty"`
	ConcurrencyGroup string             `yaml:"concurrency_group,omitempty" json:"concurrency_group,omitempty"`
	SourceFile       string             `yaml:"source_file,omitempty" json:"source_file,omitempty"`
	Notifications    NotificationConfig `yaml:"notifications,omitempty" json:"notifications,omitempty"`
	Steps            []StepConfig       `yaml:"steps" json:"steps"`
}

// UnmarshalYAML implements custom YAML unmarshaling for WorkflowConfig to support flexible duration formats.
func (w *WorkflowConfig) UnmarshalYAML(value *yaml.Node) error {
	type rawWorkflow struct {
		Version          string             `yaml:"version"`
		Name             string             `yaml:"name"`
		Description      string             `yaml:"description,omitempty"`
		Schedule         string             `yaml:"schedule,omitempty"`
		Timeout          Duration           `yaml:"timeout,omitempty"`
		MaxRetries       int                `yaml:"max_retries,omitempty"`
		RetryDelay       Duration           `yaml:"retry_delay,omitempty"`
		ConcurrencyGroup string             `yaml:"concurrency_group,omitempty"`
		SourceFile       string             `yaml:"source_file,omitempty"`
		Notifications    NotificationConfig `yaml:"notifications,omitempty"`
		Steps            []StepConfig       `yaml:"steps"`
	}

	var raw rawWorkflow
	if err := value.Decode(&raw); err != nil {
		return err
	}

	w.Version = raw.Version
	w.Name = raw.Name
	w.Description = raw.Description
	w.Schedule = raw.Schedule
	w.Timeout = raw.Timeout.Duration()
	w.MaxRetries = raw.MaxRetries
	w.RetryDelay = raw.RetryDelay.Duration()
	w.ConcurrencyGroup = raw.ConcurrencyGroup
	w.SourceFile = raw.SourceFile
	w.Notifications = raw.Notifications
	w.Steps = raw.Steps
	return nil
}

// MarshalYAML implements custom YAML marshaling for WorkflowConfig.
func (w WorkflowConfig) MarshalYAML() (any, error) {
	type rawWorkflow struct {
		Version          string             `yaml:"version"`
		Name             string             `yaml:"name"`
		Description      string             `yaml:"description,omitempty"`
		Schedule         string             `yaml:"schedule,omitempty"`
		Timeout          string             `yaml:"timeout,omitempty"`
		MaxRetries       int                `yaml:"max_retries,omitempty"`
		RetryDelay       string             `yaml:"retry_delay,omitempty"`
		ConcurrencyGroup string             `yaml:"concurrency_group,omitempty"`
		SourceFile       string             `yaml:"source_file,omitempty"`
		Notifications    NotificationConfig `yaml:"notifications,omitempty"`
		Steps            []StepConfig       `yaml:"steps"`
	}

	raw := rawWorkflow{
		Version:          w.Version,
		Name:             w.Name,
		Description:      w.Description,
		Schedule:         w.Schedule,
		MaxRetries:       w.MaxRetries,
		ConcurrencyGroup: w.ConcurrencyGroup,
		SourceFile:       w.SourceFile,
		Notifications:    w.Notifications,
		Steps:            w.Steps,
	}
	if w.Timeout > 0 {
		raw.Timeout = w.Timeout.String()
	}
	if w.RetryDelay > 0 {
		raw.RetryDelay = w.RetryDelay.String()
	}
	return raw, nil
}

// UnmarshalJSON implements custom JSON unmarshaling for WorkflowConfig.
func (w *WorkflowConfig) UnmarshalJSON(data []byte) error {
	type rawWorkflow struct {
		Version          string             `json:"version"`
		Name             string             `json:"name"`
		Description      string             `json:"description,omitempty"`
		Schedule         string             `json:"schedule,omitempty"`
		Timeout          Duration           `json:"timeout,omitempty"`
		MaxRetries       int                `json:"max_retries,omitempty"`
		RetryDelay       Duration           `json:"retry_delay,omitempty"`
		ConcurrencyGroup string             `json:"concurrency_group,omitempty"`
		SourceFile       string             `json:"source_file,omitempty"`
		Notifications    NotificationConfig `json:"notifications,omitempty"`
		Steps            []StepConfig       `json:"steps"`
	}

	var raw rawWorkflow
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	w.Version = raw.Version
	w.Name = raw.Name
	w.Description = raw.Description
	w.Schedule = raw.Schedule
	w.Timeout = raw.Timeout.Duration()
	w.MaxRetries = raw.MaxRetries
	w.RetryDelay = raw.RetryDelay.Duration()
	w.ConcurrencyGroup = raw.ConcurrencyGroup
	w.SourceFile = raw.SourceFile
	w.Notifications = raw.Notifications
	w.Steps = raw.Steps
	return nil
}

// MarshalJSON implements custom JSON marshaling for WorkflowConfig.
func (w WorkflowConfig) MarshalJSON() ([]byte, error) {
	type rawWorkflow struct {
		Version          string             `json:"version"`
		Name             string             `json:"name"`
		Description      string             `json:"description,omitempty"`
		Schedule         string             `json:"schedule,omitempty"`
		Timeout          string             `json:"timeout,omitempty"`
		MaxRetries       int                `json:"max_retries,omitempty"`
		RetryDelay       string             `json:"retry_delay,omitempty"`
		ConcurrencyGroup string             `json:"concurrency_group,omitempty"`
		SourceFile       string             `json:"source_file,omitempty"`
		Notifications    NotificationConfig `json:"notifications,omitempty"`
		Steps            []StepConfig       `json:"steps"`
	}

	raw := rawWorkflow{
		Version:          w.Version,
		Name:             w.Name,
		Description:      w.Description,
		Schedule:         w.Schedule,
		MaxRetries:       w.MaxRetries,
		ConcurrencyGroup: w.ConcurrencyGroup,
		SourceFile:       w.SourceFile,
		Notifications:    w.Notifications,
		Steps:            w.Steps,
	}
	if w.Timeout > 0 {
		raw.Timeout = w.Timeout.String()
	}
	if w.RetryDelay > 0 {
		raw.RetryDelay = w.RetryDelay.String()
	}
	return json.Marshal(raw)
}
