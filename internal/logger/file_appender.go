package logger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	// ErrInvalidRunID is returned when the run ID contains illegal path characters.
	ErrInvalidRunID = errors.New("invalid run ID: path traversal characters not allowed")
	// ErrNegativeMaxBytes is returned when maxBytes passed to ReadLogsTail is negative.
	ErrNegativeMaxBytes = errors.New("maxBytes cannot be negative")
)

// FileAppender manages raw disk log files under a configurable base directory.
type FileAppender struct {
	baseDir   string
	fileLocks sync.Map // map[string]*sync.Mutex
}

// NewFileAppender creates a new FileAppender for the specified directory.
// If baseDir is empty, it defaults to "~/.glowkron/logs".
// Tilde expansion ("~") is supported for home directory paths.
func NewFileAppender(baseDir string) (*FileAppender, error) {
	if baseDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to determine user home directory: %w", err)
		}
		baseDir = filepath.Join(home, ".glowkron", "logs")
	} else if strings.HasPrefix(baseDir, "~/") || baseDir == "~" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to determine user home directory: %w", err)
		}
		if baseDir == "~" {
			baseDir = home
		} else {
			baseDir = filepath.Join(home, baseDir[2:])
		}
	}

	cleanDir := filepath.Clean(baseDir)
	if err := os.MkdirAll(cleanDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %q: %w", cleanDir, err)
	}

	return &FileAppender{
		baseDir: cleanDir,
	}, nil
}

// BaseDir returns the resolved base directory for logs.
func (fa *FileAppender) BaseDir() string {
	return fa.baseDir
}

// LogFilePath returns the expected log file path for a runID.
// It verifies that runID is safe and does not contain path traversal elements.
func (fa *FileAppender) LogFilePath(runID string) (string, error) {
	if runID == "" {
		return "", errors.New("runID cannot be empty")
	}
	if strings.Contains(runID, "/") || strings.Contains(runID, "\\") || strings.Contains(runID, "..") {
		return "", ErrInvalidRunID
	}
	return filepath.Join(fa.baseDir, runID+".log"), nil
}

func (fa *FileAppender) getRunMutex(runID string) *sync.Mutex {
	v, _ := fa.fileLocks.LoadOrStore(runID, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// Write appends bytes p to the disk log file for runID thread-safely.
func (fa *FileAppender) Write(runID string, p []byte) (n int, err error) {
	path, err := fa.LogFilePath(runID)
	if err != nil {
		return 0, err
	}

	mtx := fa.getRunMutex(runID)
	mtx.Lock()
	defer mtx.Unlock()

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open log file %q: %w", path, err)
	}
	defer f.Close()

	return f.Write(p)
}

// OpenWriter returns an append-mode io.WriteCloser for the specified runID.
func (fa *FileAppender) OpenWriter(runID string) (io.WriteCloser, error) {
	path, err := fa.LogFilePath(runID)
	if err != nil {
		return nil, err
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file %q: %w", path, err)
	}

	return &runFileWriter{
		appender: fa,
		runID:    runID,
		file:     f,
	}, nil
}

type runFileWriter struct {
	appender *FileAppender
	runID    string
	file     *os.File
	mu       sync.Mutex
	closed   bool
}

func (w *runFileWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, errors.New("writer already closed")
	}

	mtx := w.appender.getRunMutex(w.runID)
	mtx.Lock()
	defer mtx.Unlock()

	return w.file.Write(p)
}

func (w *runFileWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true
	return w.file.Close()
}

// ReadLogs reads the entire raw log content from disk for runID.
func (fa *FileAppender) ReadLogs(runID string) ([]byte, error) {
	path, err := fa.LogFilePath(runID)
	if err != nil {
		return nil, err
	}

	mtx := fa.getRunMutex(runID)
	mtx.Lock()
	defer mtx.Unlock()

	return os.ReadFile(path)
}

// ReadLogsTail reads up to maxBytes from the tail of the log file for runID.
// If maxBytes <= 0, an empty slice is returned.
// If file size is less than or equal to maxBytes, the entire file content is returned.
func (fa *FileAppender) ReadLogsTail(runID string, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, ErrNegativeMaxBytes
	}
	if maxBytes == 0 {
		return []byte{}, nil
	}

	path, err := fa.LogFilePath(runID)
	if err != nil {
		return nil, err
	}

	mtx := fa.getRunMutex(runID)
	mtx.Lock()
	defer mtx.Unlock()

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat log file: %w", err)
	}

	fileSize := fi.Size()
	if fileSize == 0 {
		return []byte{}, nil
	}

	var offset int64 = 0
	readLen := fileSize
	if fileSize > maxBytes {
		offset = fileSize - maxBytes
		readLen = maxBytes
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek in log file: %w", err)
	}

	buf := make([]byte, readLen)
	_, err = io.ReadFull(f, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read tail from log file: %w", err)
	}

	return buf, nil
}
