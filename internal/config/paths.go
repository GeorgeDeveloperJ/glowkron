package config

import (
	"os"
	"path/filepath"
)

const (
	DefaultPort        = 8080
	DefaultConcurrency = 4
	DefaultDebounceMs  = 150
)

// Paths holds the resolved canonical filesystem paths used by GlowKron.
type Paths struct {
	HomeDir      string
	ConfigFile   string
	DBFile       string
	WorkflowsDir string
	LogsDir      string
}

// ResolvePaths resolves canonical directories based on home override or environment.
// If homeOverride is empty, it defaults to $HOME/.glowkron (or ./.glowkron if HOME is unset).
func ResolvePaths(homeOverride string) (*Paths, error) {
	home := homeOverride
	if home == "" {
		home = os.Getenv("GLOWKRON_HOME")
	}
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			home = ".glowkron"
		} else {
			home = filepath.Join(userHome, ".glowkron")
		}
	}

	absHome, err := filepath.Abs(home)
	if err != nil {
		absHome = home
	}

	return &Paths{
		HomeDir:      absHome,
		ConfigFile:   filepath.Join(absHome, "config.yaml"),
		DBFile:       filepath.Join(absHome, "glowkron.db"),
		WorkflowsDir: filepath.Join(absHome, "workflows"),
		LogsDir:      filepath.Join(absHome, "logs"),
	}, nil
}

// EnsureDirs creates all necessary directory structures if they do not exist.
func (p *Paths) EnsureDirs() error {
	dirs := []string{
		p.HomeDir,
		p.WorkflowsDir,
		p.LogsDir,
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0755); err != nil {
			return err
		}
	}
	return nil
}
