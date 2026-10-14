package notifier

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

const (
	DefaultQueueCapacity = 1024
	DefaultWorkerCount   = 2
	DefaultTimeout       = 5 * time.Second
	DefaultMaxRetries    = 3
	DefaultInitialDelay  = 200 * time.Millisecond
)

// Dispatcher coordinates non-blocking, asynchronous delivery of webhook alerts
// with retry backoff and multi-platform payload serialization.
type Dispatcher struct {
	queue        chan Event
	workers      int
	timeout      time.Duration
	maxRetries   int
	initialDelay time.Duration
	client       *http.Client
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	closeOnce    sync.Once
}

type Option func(*Dispatcher)

func WithQueueCapacity(cap int) Option {
	return func(d *Dispatcher) {
		d.queue = make(chan Event, cap)
	}
}

func WithWorkers(workers int) Option {
	return func(d *Dispatcher) {
		if workers > 0 {
			d.workers = workers
		}
	}
}

func WithTimeout(t time.Duration) Option {
	return func(d *Dispatcher) {
		if t > 0 {
			d.timeout = t
			d.client.Timeout = t
		}
	}
}

func WithMaxRetries(retries int) Option {
	return func(d *Dispatcher) {
		if retries >= 0 {
			d.maxRetries = retries
		}
	}
}

func WithInitialDelay(delay time.Duration) Option {
	return func(d *Dispatcher) {
		if delay >= 0 {
			d.initialDelay = delay
		}
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(d *Dispatcher) {
		if client != nil {
			d.client = client
		}
	}
}

// NewDispatcher initializes an asynchronous webhook dispatcher and starts background workers.
func NewDispatcher(opts ...Option) *Dispatcher {
	ctx, cancel := context.WithCancel(context.Background())

	d := &Dispatcher{
		queue:        make(chan Event, DefaultQueueCapacity),
		workers:      DefaultWorkerCount,
		timeout:      DefaultTimeout,
		maxRetries:   DefaultMaxRetries,
		initialDelay: DefaultInitialDelay,
		client: &http.Client{
			Timeout: DefaultTimeout,
		},
		ctx:    ctx,
		cancel: cancel,
	}

	for _, opt := range opts {
		opt(d)
	}

	// Start background workers
	d.wg.Add(d.workers)
	for i := 0; i < d.workers; i++ {
		go d.workerLoop()
	}

	return d
}

// Dispatch queues an event for non-blocking asynchronous delivery.
// Returns true if queued, or false if the queue is full (dropping event safely).
func (d *Dispatcher) Dispatch(e Event) bool {
	if !ShouldDispatch(e.Config, e.Status) {
		return false
	}

	select {
	case d.queue <- e:
		return true
	default:
		// Queue saturated; drop safely to protect engine execution latency
		return false
	}
}

func (d *Dispatcher) workerLoop() {
	defer d.wg.Done()

	for {
		select {
		case <-d.ctx.Done():
			return
		case event, ok := <-d.queue:
			if !ok {
				return
			}
			_ = d.sendWithRetry(d.ctx, event)
		}
	}
}

func (d *Dispatcher) sendWithRetry(ctx context.Context, e Event) error {
	payload, err := d.formatPayload(e)
	if err != nil {
		return fmt.Errorf("failed to format webhook payload: %w", err)
	}

	var lastErr error
	delay := d.initialDelay

	for attempt := 1; attempt <= d.maxRetries; attempt++ {
		reqCtx, reqCancel := context.WithTimeout(ctx, d.timeout)
		req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, e.WebhookURL, bytes.NewReader(payload))
		if err != nil {
			reqCancel()
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "GlowKron-Notifier/1.0")

		resp, err := d.client.Do(req)
		if err != nil {
			reqCancel()
			lastErr = err
		} else {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			reqCancel()

			// Success
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}

			// Non-retryable client errors (except 429 Too Many Requests)
			if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
				return fmt.Errorf("webhook rejected with non-retryable HTTP %d", resp.StatusCode)
			}

			lastErr = fmt.Errorf("webhook responded with HTTP %d", resp.StatusCode)
		}

		if attempt < d.maxRetries {
			// Apply exponential backoff with randomized jitter (0% to +30%)
			jitter := time.Duration(cryptoRandInt64(int64(delay / 3)))
			backoff := delay + jitter

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			delay *= 2
		}
	}

	return fmt.Errorf("webhook dispatch failed after %d attempts: %w", d.maxRetries, lastErr)
}

func (d *Dispatcher) formatPayload(e Event) ([]byte, error) {
	platform := DetectPlatform(e.WebhookURL)
	switch platform {
	case PlatformGoogleChat:
		return FormatGoogleChat(e)
	case PlatformDiscord:
		return FormatDiscord(e)
	case PlatformTelegram:
		return FormatTelegram(e)
	default:
		return FormatGeneric(e)
	}
}

func (d *Dispatcher) Close() error {
	var err error
	d.closeOnce.Do(func() {
		close(d.queue)
		d.cancel()
		d.wg.Wait()
		d.client.CloseIdleConnections()
	})
	return err
}

func cryptoRandInt64(max int64) int64 {
	if max <= 0 {
		return 0
	}
	nBig, err := rand.Int(rand.Reader, big.NewInt(max))
	if err != nil {
		return 0
	}
	return nBig.Int64()
}
