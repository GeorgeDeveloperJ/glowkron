package notifier

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
)

// GoogleChatCardV2 represents the Card v2 schema for Google Chat incoming webhooks.
type GoogleChatCardV2 struct {
	CardsV2 []GoogleChatCardWrapper `json:"cardsV2"`
}

type GoogleChatCardWrapper struct {
	CardID string         `json:"cardId"`
	Card   GoogleChatCard `json:"card"`
}

type GoogleChatCard struct {
	Header   GoogleChatHeader    `json:"header"`
	Sections []GoogleChatSection `json:"sections"`
}

type GoogleChatHeader struct {
	Title    string `json:"title"`
	Subtitle string `json:"subtitle"`
	ImageURL string `json:"imageUrl,omitempty"`
}

type GoogleChatSection struct {
	Header  string             `json:"header,omitempty"`
	Widgets []GoogleChatWidget `json:"widgets"`
}

type GoogleChatWidget struct {
	DecoratedText *GoogleChatDecoratedText `json:"decoratedText,omitempty"`
}

type GoogleChatDecoratedText struct {
	TopLabel string `json:"topLabel,omitempty"`
	Text     string `json:"text"`
}

// FormatGoogleChat formats an Event into a Google Chat Card v2 payload.
func FormatGoogleChat(e Event) ([]byte, error) {
	statusEmoji := "🟢"
	if e.Status == store.JobRunFailed || e.Status == store.JobRunCancelled {
		statusEmoji = "🔴"
	}

	title := fmt.Sprintf("%s GlowKron: %s", statusEmoji, e.JobName)
	if e.JobName == "" {
		title = fmt.Sprintf("%s GlowKron: %s", statusEmoji, e.JobID)
	}

	subtitle := fmt.Sprintf("Workflow %s • %s", e.Status, e.Duration.Round(time.Millisecond))

	widgets := []GoogleChatWidget{
		{DecoratedText: &GoogleChatDecoratedText{TopLabel: "Run ID", Text: e.RunID}},
		{DecoratedText: &GoogleChatDecoratedText{TopLabel: "Status", Text: string(e.Status)}},
		{DecoratedText: &GoogleChatDecoratedText{TopLabel: "Exit Code", Text: fmt.Sprintf("%d", e.ExitCode)}},
		{DecoratedText: &GoogleChatDecoratedText{TopLabel: "Duration", Text: e.Duration.Round(time.Millisecond).String()}},
	}

	if e.ErrorMessage != "" {
		widgets = append(widgets, GoogleChatWidget{
			DecoratedText: &GoogleChatDecoratedText{TopLabel: "Error", Text: e.ErrorMessage},
		})
	}

	card := GoogleChatCardV2{
		CardsV2: []GoogleChatCardWrapper{
			{
				CardID: "glowkron-" + e.RunID,
				Card: GoogleChatCard{
					Header: GoogleChatHeader{
						Title:    title,
						Subtitle: subtitle,
					},
					Sections: []GoogleChatSection{
						{
							Header:  "Execution Summary",
							Widgets: widgets,
						},
					},
				},
			},
		},
	}

	return json.Marshal(card)
}
