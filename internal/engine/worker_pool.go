package engine

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	// ErrPoolClosed is returned when attempting to acquire or submit work to a closed worker pool.
	ErrPoolClosed = errors.New("worker pool is closed")
)

// WorkerPool coordinates bounded concurrency using a buffered semaphore channel.
type WorkerPool struct {
	maxConcurrency int
	sem            chan struct{}
	running        atomic.Int32
	peakRunning    atomic.Int32
	wg             sync.WaitGroup
	mu             sync.Mutex
	closed         bool
}

// NewWorkerPool initializes a WorkerPool with the specified maximum concurrency limit.
// If maxConcurrency <= 0, it defaults to runtime.NumCPU().
func NewWorkerPool(maxConcurrency int) *WorkerPool {
	if maxConcurrency <= 0 {
		maxConcurrency = runtime.NumCPU()
	}
	return &WorkerPool{
		maxConcurrency: maxConcurrency,
		sem:            make(chan struct{}, maxConcurrency),
	}
}

// MaxConcurrency returns the maximum number of concurrent worker slots allowed.
func (p *WorkerPool) MaxConcurrency() int {
	return p.maxConcurrency
}

// Running returns the number of currently active workers.
func (p *WorkerPool) Running() int {
	return int(p.running.Load())
}

// PeakRunning returns the maximum concurrent workers observed over the pool lifecycle.
func (p *WorkerPool) PeakRunning() int {
	return int(p.peakRunning.Load())
}

// Acquire reserves an execution slot from the semaphore pool, blocking until available
// or until ctx is canceled.
func (p *WorkerPool) Acquire(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrPoolClosed
	}
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.sem <- struct{}{}:
		curr := p.running.Add(1)
		for {
			peak := p.peakRunning.Load()
			if curr <= peak || p.peakRunning.CompareAndSwap(peak, curr) {
				break
			}
		}
		p.wg.Add(1)
		return nil
	}
}

// Release yields a previously acquired worker slot back to the pool.
func (p *WorkerPool) Release() {
	p.running.Add(-1)
	<-p.sem
	p.wg.Done()
}

// Submit dispatches a task to execute in a goroutine once a worker slot is acquired.
func (p *WorkerPool) Submit(ctx context.Context, task func()) error {
	if err := p.Acquire(ctx); err != nil {
		return err
	}
	go func() {
		defer p.Release()
		task()
	}()
	return nil
}

// Wait blocks until all currently executing tasks in the pool have finished.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

// Close closes the worker pool and waits for active workers to complete.
func (p *WorkerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	p.Wait()
}
