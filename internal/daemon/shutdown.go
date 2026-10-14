package daemon

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ShutdownCoordinator handles OS signal trapping and orchestrates phased graceful shutdown.
type ShutdownCoordinator struct {
	signals chan os.Signal
	done    chan struct{}
}

// NewShutdownCoordinator creates a ShutdownCoordinator listening for SIGINT and SIGTERM.
func NewShutdownCoordinator() *ShutdownCoordinator {
	c := &ShutdownCoordinator{
		signals: make(chan os.Signal, 1),
		done:    make(chan struct{}),
	}
	signal.Notify(c.signals, syscall.SIGINT, syscall.SIGTERM)
	return c
}

// Wait blocks until an OS termination signal is received or the coordinator is explicitly stopped.
func (c *ShutdownCoordinator) Wait() os.Signal {
	select {
	case sig := <-c.signals:
		return sig
	case <-c.done:
		return nil
	}
}

// Signals returns the read-only signal channel for select multiplexing.
func (c *ShutdownCoordinator) Signals() <-chan os.Signal {
	return c.signals
}

// Close unregisters OS signal notifications.
func (c *ShutdownCoordinator) Close() {
	signal.Stop(c.signals)
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

// GracefulTimeoutContext creates a context bound to a shutdown grace period.
func GracefulTimeoutContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}
