package notifier

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// TelegramPayload represents the payload sent to Telegram sendMessage endpoint.
type TelegramPayload struct {
	Text      string `json:"text"`
	ParseMode string `json:"parse_mode"`
}

// FormatTelegram formats an Event into a Markdown-formatted Telegram payload.
func FormatTelegram(e Event) ([]byte, error) {
	statusEmoji := "🟢"
	if e.Status == store.JobRunFailed || e.Status == store.JobRunCancelled {
		statusEmoji = "🔴"
	}

	jobTitle := e.JobName
	if jobTitle == "" {
		jobTitle = e.JobID
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s *GlowKron Workflow Alert*\n\n", statusEmoji))
	sb.WriteString(fmt.Sprintf("*Job:* `%s`\n", jobTitle))
	sb.WriteString(fmt.Sprintf("*Status:* `%s`\n", e.Status))
	sb.WriteString(fmt.Sprintf("*Run ID:* `%s`\n", e.RunID))
	sb.WriteString(fmt.Sprintf("*Exit Code:* `%d`\n", e.ExitCode))
	sb.WriteString(fmt.Sprintf("*Duration:* `%s`\n", e.Duration.Round(time.Millisecond)))

	if e.ErrorMessage != "" {
		sb.WriteString(fmt.Sprintf("\n*Error:*\n```\n%s\n```", e.ErrorMessage))
	}

	payload := TelegramPayload{
		Text:      sb.String(),
		ParseMode: "Markdown",
	}

	return json.Marshal(payload)
}
