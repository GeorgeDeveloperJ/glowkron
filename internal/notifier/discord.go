package notifier

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// DiscordWebhookPayload represents the Discord Webhook execution payload.
type DiscordWebhookPayload struct {
	Username  string         `json:"username"`
	AvatarURL string         `json:"avatar_url,omitempty"`
	Embeds    []DiscordEmbed `json:"embeds"`
}

type DiscordEmbed struct {
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Color       int            `json:"color"`
	Fields      []DiscordField `json:"fields"`
	Timestamp   string         `json:"timestamp"`
	Footer      *DiscordFooter `json:"footer,omitempty"`
}

type DiscordField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

type DiscordFooter struct {
	Text string `json:"text"`
}

const (
	DiscordColorGreen = 0x2ecc71 // 3066993
	DiscordColorRed   = 0xe74c3c // 15158332
)

// FormatDiscord formats an Event into a Discord Webhook Embed payload.
func FormatDiscord(e Event) ([]byte, error) {
	color := DiscordColorGreen
	statusIcon := "✅"
	if e.Status == store.JobRunFailed || e.Status == store.JobRunCancelled {
		color = DiscordColorRed
		statusIcon = "❌"
	}

	jobTitle := e.JobName
	if jobTitle == "" {
		jobTitle = e.JobID
	}

	fields := []DiscordField{
		{Name: "Run ID", Value: fmt.Sprintf("`%s`", e.RunID), Inline: true},
		{Name: "Exit Code", Value: fmt.Sprintf("%d", e.ExitCode), Inline: true},
		{Name: "Duration", Value: e.Duration.Round(time.Millisecond).String(), Inline: true},
	}

	if e.ErrorMessage != "" {
		fields = append(fields, DiscordField{
			Name:   "Error",
			Value:  fmt.Sprintf("```\n%s\n```", e.ErrorMessage),
			Inline: false,
		})
	}

	payload := DiscordWebhookPayload{
		Username: "GlowKron",
		Embeds: []DiscordEmbed{
			{
				Title:       fmt.Sprintf("%s Workflow: %s", statusIcon, jobTitle),
				Description: fmt.Sprintf("Execution finished with status **%s**", e.Status),
				Color:       color,
				Fields:      fields,
				Timestamp:   e.Timestamp.UTC().Format(time.RFC3339),
				Footer: &DiscordFooter{
					Text: "GlowKron Workflow Orchestrator",
				},
			},
		},
	}

	return json.Marshal(payload)
}
