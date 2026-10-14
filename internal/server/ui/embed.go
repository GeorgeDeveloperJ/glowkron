package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var staticFS embed.FS

// Handler returns an http.Handler that serves the embedded web dashboard and static assets.
func Handler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}

// GetAsset reads a named asset from the embedded static filesystem.
func GetAsset(name string) ([]byte, error) {
	return staticFS.ReadFile("static/" + name)
}
