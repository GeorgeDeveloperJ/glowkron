package ui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/GeorgeDeveloperJ/glowkron/internal/server/ui"
)

func TestEmbeddedFilesystem_ServesAssets(t *testing.T) {
	ts := httptest.NewServer(ui.Handler())
	defer ts.Close()

	client := ts.Client()

	tests := []struct {
		name             string
		url              string
		expectedMIMEType string
	}{
		{
			name:             "index.html via redirect or root",
			url:              ts.URL + "/index.html",
			expectedMIMEType: "text/html",
		},
		{
			name:             "index.html at root",
			url:              ts.URL + "/",
			expectedMIMEType: "text/html",
		},
		{
			name:             "app.css",
			url:              ts.URL + "/app.css",
			expectedMIMEType: "text/css",
		},
		{
			name:             "app.js",
			url:              ts.URL + "/app.js",
			expectedMIMEType: "javascript",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := client.Get(tt.url)
			if err != nil {
				t.Fatalf("GET %s failed: %v", tt.url, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected status 200 OK for %s, got %d", tt.url, resp.StatusCode)
			}

			contentType := resp.Header.Get("Content-Type")
			if !strings.Contains(contentType, tt.expectedMIMEType) {
				t.Errorf("expected Content-Type containing %q for %s, got %q", tt.expectedMIMEType, tt.url, contentType)
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("failed to read response body: %v", err)
			}
			if len(body) == 0 {
				t.Errorf("expected non-empty body for %s, got 0 bytes", tt.url)
			}
		})
	}
}

func TestGetAsset(t *testing.T) {
	assets := []string{"index.html", "app.css", "app.js"}

	for _, asset := range assets {
		t.Run(asset, func(t *testing.T) {
			data, err := ui.GetAsset(asset)
			if err != nil {
				t.Fatalf("unexpected error reading asset %s: %v", asset, err)
			}
			if len(data) == 0 {
				t.Errorf("expected non-empty content for asset %s", asset)
			}
		})
	}

	// Non-existent asset
	_, err := ui.GetAsset("nonexistent.txt")
	if err == nil {
		t.Error("expected error reading nonexistent asset, got nil")
	}
}

func TestEmbeddedUI_SpecRequirements(t *testing.T) {
	htmlBytes, err := ui.GetAsset("index.html")
	if err != nil {
		t.Fatalf("failed to read index.html: %v", err)
	}
	html := string(htmlBytes)

	// 1. Zero External CDN Check: no external http(s) links in index.html
	if strings.Contains(html, "http://") || strings.Contains(html, "https://") {
		t.Errorf("index.html must have ZERO external dependencies, found external link: %s", html)
	}
	if strings.Contains(html, "fonts.googleapis.com") || strings.Contains(html, "cdnjs.cloudflare.com") {
		t.Errorf("index.html contains external CDN font/script references")
	}

	cssBytes, err := ui.GetAsset("app.css")
	if err != nil {
		t.Fatalf("failed to read app.css: %v", err)
	}
	css := string(cssBytes)

	// 2. CSS Status Tokens Check (PENDING, RUNNING, COMPLETED, FAILED, SKIPPED)
	requiredTokens := []string{
		"status-pending",
		"status-running",
		"status-completed",
		"status-failed",
		"status-skipped",
		"pulse-glow",
		"terminal-card",
	}
	for _, tok := range requiredTokens {
		if !strings.Contains(css, tok) {
			t.Errorf("app.css missing required token %q", tok)
		}
	}

	jsBytes, err := ui.GetAsset("app.js")
	if err != nil {
		t.Fatalf("failed to read app.js: %v", err)
	}
	js := string(jsBytes)

	// 3. JS Features Check (SSE stream, auto-scroll, stages, reconnection, filters)
	requiredJS := []string{
		"EventSource",
		"/api/runs/",
		"/stream",
		"computeTopologicalStages",
		"autoScroll",
		"reconnect",
		"statusFilter",
		"showToast",
	}
	for _, tok := range requiredJS {
		if !strings.Contains(js, tok) {
			t.Errorf("app.js missing required feature token %q", tok)
		}
	}
}
