package notifier_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/notifier"
	"github.com/GeorgeDeveloperJ/glowkron/internal/store"
	"github.com/GeorgeDeveloperJ/glowkron/internal/workflow"
)

func TestPlatformDetection(t *testing.T) {
	tests := []struct {
		url      string
		expected notifier.Platform
	}{
		{"https://chat.googleapis.com/v1/spaces/XYZ/messages?key=123", notifier.PlatformGoogleChat},
		{"https://discord.com/api/webhooks/123/XYZ", notifier.PlatformDiscord},
		{"https://discordapp.com/api/webhooks/456/ABC", notifier.PlatformDiscord},
		{"https://api.telegram.org/bot123/sendMessage", notifier.PlatformTelegram},
		{"https://webhook.site/my-custom-endpoint", notifier.PlatformGeneric},
	}

	for _, tt := range tests {
		actual := notifier.DetectPlatform(tt.url)
		if actual != tt.expected {
			t.Errorf("DetectPlatform(%q) = %s, expected %s", tt.url, actual, tt.expected)
		}
	}
}

func TestShouldDispatch(t *testing.T) {
	cfg := workflow.NotificationConfig{
		WebhookURL: "https://example.com/webhook",
		OnSuccess:  true,
		OnFailure:  true,
	}

	if !notifier.ShouldDispatch(cfg, store.JobRunCompleted) {
		t.Errorf("expected dispatch for COMPLETED when OnSuccess=true")
	}
	if !notifier.ShouldDispatch(cfg, store.JobRunFailed) {
		t.Errorf("expected dispatch for FAILED when OnFailure=true")
	}

	cfgNoSuccess := workflow.NotificationConfig{
		WebhookURL: "https://example.com/webhook",
		OnSuccess:  false,
		OnFailure:  true,
	}
	if notifier.ShouldDispatch(cfgNoSuccess, store.JobRunCompleted) {
		t.Errorf("expected no dispatch for COMPLETED when OnSuccess=false")
	}

	cfgNoURL := workflow.NotificationConfig{
		WebhookURL: "",
		OnSuccess:  true,
	}
	if notifier.ShouldDispatch(cfgNoURL, store.JobRunCompleted) {
		t.Errorf("expected no dispatch when WebhookURL is empty")
	}
}

func TestFormatters(t *testing.T) {
	baseEvent := notifier.Event{
		Type:         notifier.EventWorkflowFailed,
		Timestamp:    time.Now(),
		JobID:        "backup-db",
		JobName:      "Backup Database",
		RunID:        "run-abc-123",
		Status:       store.JobRunFailed,
		ExitCode:     1,
		Duration:     1250 * time.Millisecond,
		ErrorMessage: "database connection refused",
	}

	// 1. Generic Formatter
	genericBytes, err := notifier.FormatGeneric(baseEvent)
	if err != nil {
		t.Fatalf("FormatGeneric error: %v", err)
	}
	var genericMap map[string]any
	if err := json.Unmarshal(genericBytes, &genericMap); err != nil {
		t.Fatalf("invalid generic json: %v", err)
	}
	if genericMap["job_id"] != "backup-db" || genericMap["status"] != "FAILED" {
		t.Errorf("unexpected generic payload: %+v", genericMap)
	}

	// 2. Google Chat Formatter
	gchatBytes, err := notifier.FormatGoogleChat(baseEvent)
	if err != nil {
		t.Fatalf("FormatGoogleChat error: %v", err)
	}
	var gchatMap map[string]any
	if err := json.Unmarshal(gchatBytes, &gchatMap); err != nil {
		t.Fatalf("invalid google chat json: %v", err)
	}
	if _, ok := gchatMap["cardsV2"]; !ok {
		t.Errorf("missing cardsV2 in google chat payload")
	}

	// 3. Discord Formatter
	discordBytes, err := notifier.FormatDiscord(baseEvent)
	if err != nil {
		t.Fatalf("FormatDiscord error: %v", err)
	}
	var discordMap map[string]any
	if err := json.Unmarshal(discordBytes, &discordMap); err != nil {
		t.Fatalf("invalid discord json: %v", err)
	}
	embeds, ok := discordMap["embeds"].([]any)
	if !ok || len(embeds) == 0 {
		t.Fatalf("missing embeds in discord payload")
	}
	firstEmbed := embeds[0].(map[string]any)
	if firstEmbed["color"] != float64(notifier.DiscordColorRed) {
		t.Errorf("expected red color for failed event, got %v", firstEmbed["color"])
	}

	// 4. Telegram Formatter
	telegramBytes, err := notifier.FormatTelegram(baseEvent)
	if err != nil {
		t.Fatalf("FormatTelegram error: %v", err)
	}
	var telegramMap map[string]any
	if err := json.Unmarshal(telegramBytes, &telegramMap); err != nil {
		t.Fatalf("invalid telegram json: %v", err)
	}
	text, _ := telegramMap["text"].(string)
	if !strings.Contains(text, "Backup Database") || !strings.Contains(text, "database connection refused") {
		t.Errorf("unexpected telegram message text: %q", text)
	}
}

func TestDispatcher_RetryAndSuccess(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&attempts, 1)
		if count == 1 {
			http.Error(w, "transient error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	d := notifier.NewDispatcher(
		notifier.WithInitialDelay(10*time.Millisecond),
		notifier.WithMaxRetries(3),
		notifier.WithTimeout(1*time.Second),
	)
	defer d.Close()

	ev := notifier.Event{
		Type:       notifier.EventWorkflowCompleted,
		JobID:      "test-job",
		RunID:      "run-999",
		Status:     store.JobRunCompleted,
		WebhookURL: server.URL,
		Config: workflow.NotificationConfig{
			WebhookURL: server.URL,
			OnSuccess:  true,
		},
	}

	if queued := d.Dispatch(ev); !queued {
		t.Fatalf("expected event to be queued successfully")
	}

	// Wait for worker retry loop to deliver
	for i := 0; i < 20; i++ {
		if atomic.LoadInt32(&attempts) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if atomic.LoadInt32(&attempts) != 2 {
		t.Errorf("expected 2 attempts before success, got %d", atomic.LoadInt32(&attempts))
	}
}

func TestDispatcher_NonRetryable400(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		http.Error(w, "bad payload", http.StatusBadRequest)
	}))
	defer server.Close()

	d := notifier.NewDispatcher(
		notifier.WithInitialDelay(10*time.Millisecond),
		notifier.WithMaxRetries(3),
	)
	defer d.Close()

	ev := notifier.Event{
		Type:       notifier.EventWorkflowFailed,
		JobID:      "bad-job",
		RunID:      "run-400",
		Status:     store.JobRunFailed,
		WebhookURL: server.URL,
		Config: workflow.NotificationConfig{
			WebhookURL: server.URL,
			OnFailure:  true,
		},
	}

	d.Dispatch(ev)

	time.Sleep(100 * time.Millisecond)

	// Should not retry on 400 Bad Request
	if atomic.LoadInt32(&attempts) != 1 {
		t.Errorf("expected exactly 1 attempt on HTTP 400, got %d", atomic.LoadInt32(&attempts))
	}
}
