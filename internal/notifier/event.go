package notifier

import (
	"strings"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

// Platform identifies the target webhook notification provider.
type Platform string

const (
	PlatformGoogleChat Platform = "google_chat"
	PlatformDiscord    Platform = "discord"
	PlatformTelegram   Platform = "telegram"
	PlatformGeneric    Platform = "generic"
)

// EventType categorizes the notification lifecycle event.
type EventType string

const (
	EventWorkflowStarted   EventType = "workflow.started"
	EventWorkflowCompleted EventType = "workflow.completed"
	EventWorkflowFailed    EventType = "workflow.failed"
	EventRetryExhausted    EventType = "workflow.retry_exhausted"
)

// Event encapsulates the metadata delivered to webhook notification endpoints.
type Event struct {
	Type         EventType                   `json:"event"`
	Timestamp    time.Time                   `json:"timestamp"`
	JobID        string                      `json:"job_id"`
	JobName      string                      `json:"job_name"`
	RunID        string                      `json:"run_id"`
	Status       store.JobRunStatus          `json:"status"`
	ExitCode     int                         `json:"exit_code"`
	Duration     time.Duration               `json:"duration"`
	ErrorMessage string                      `json:"error,omitempty"`
	Attempts     int                         `json:"attempts"`
	MaxRetries   int                         `json:"max_retries"`
	WebhookURL   string                      `json:"-"`
	Config       workflow.NotificationConfig `json:"-"`
}

// DetectPlatform determines the webhook platform based on the target URL.
func DetectPlatform(webhookURL string) Platform {
	lower := strings.ToLower(webhookURL)
	if strings.Contains(lower, "chat.googleapis.com") {
		return PlatformGoogleChat
	}
	if strings.Contains(lower, "discord.com/api/webhooks") || strings.Contains(lower, "discordapp.com/api/webhooks") {
		return PlatformDiscord
	}
	if strings.Contains(lower, "api.telegram.org") {
		return PlatformTelegram
	}
	return PlatformGeneric
}

// ShouldDispatch determines if an event should be sent based on workflow notification rules.
func ShouldDispatch(cfg workflow.NotificationConfig, status store.JobRunStatus) bool {
	if cfg.WebhookURL == "" {
		return false
	}
	if status == store.JobRunCompleted && cfg.OnSuccess {
		return true
	}
	if (status == store.JobRunFailed || status == store.JobRunCancelled) && cfg.OnFailure {
		return true
	}
	return false
}
