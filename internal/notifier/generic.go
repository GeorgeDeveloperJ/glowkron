package notifier

import (
	"encoding/json"
	"time"
)

// GenericPayload defines the standardized JSON schema sent to generic webhook receivers.
type GenericPayload struct {
	Event        string `json:"event"`
	Timestamp    string `json:"timestamp"`
	JobID        string `json:"job_id"`
	JobName      string `json:"job_name"`
	RunID        string `json:"run_id"`
	Status       string `json:"status"`
	ExitCode     int    `json:"exit_code"`
	DurationMs   int64  `json:"duration_ms"`
	Duration     string `json:"duration"`
	ErrorMessage string `json:"error,omitempty"`
	Attempts     int    `json:"attempts,omitempty"`
}

// FormatGeneric marshals an Event into the generic webhook JSON representation.
func FormatGeneric(e Event) ([]byte, error) {
	payload := GenericPayload{
		Event:        string(e.Type),
		Timestamp:    e.Timestamp.UTC().Format(time.RFC3339),
		JobID:        e.JobID,
		JobName:      e.JobName,
		RunID:        e.RunID,
		Status:       string(e.Status),
		ExitCode:     e.ExitCode,
		DurationMs:   e.Duration.Milliseconds(),
		Duration:     e.Duration.Round(time.Millisecond).String(),
		ErrorMessage: e.ErrorMessage,
		Attempts:     e.Attempts,
	}
	return json.Marshal(payload)
}
