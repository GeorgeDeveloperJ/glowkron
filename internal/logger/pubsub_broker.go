package logger

import (
	"sync"

	"github.com/google/uuid"
)

// DefaultBufferSize is the default channel capacity for each log subscriber.
const DefaultBufferSize = 256

type subscriber struct {
	id        string
	runID     string
	ch        chan []byte
	closeOnce sync.Once
}

// LogBroker broadcasts real-time log chunks to in-memory subscribers (SSE, CLI).
type LogBroker struct {
	mu          sync.RWMutex
	bufferSize  int
	subscribers map[string]map[string]*subscriber // runID -> subID -> subscriber
}

// NewLogBroker creates a new in-memory pub/sub broker with optional buffer size.
func NewLogBroker(bufferSize ...int) *LogBroker {
	bufSize := DefaultBufferSize
	if len(bufferSize) > 0 && bufferSize[0] > 0 {
		bufSize = bufferSize[0]
	}

	return &LogBroker{
		bufferSize:  bufSize,
		subscribers: make(map[string]map[string]*subscriber),
	}
}

// Subscribe registers a new subscriber channel for a specific runID.
// It returns a unique subID, a receive-only channel, and a cancel function.
func (b *LogBroker) Subscribe(runID string) (subID string, ch <-chan []byte, cancel func()) {
	if runID == "" {
		emptyCh := make(chan []byte)
		close(emptyCh)
		return "", emptyCh, func() {}
	}

	subID = uuid.NewString()
	subCh := make(chan []byte, b.bufferSize)

	sub := &subscriber{
		id:    subID,
		runID: runID,
		ch:    subCh,
	}

	b.mu.Lock()
	subsMap, ok := b.subscribers[runID]
	if !ok {
		subsMap = make(map[string]*subscriber)
		b.subscribers[runID] = subsMap
	}
	subsMap[subID] = sub
	b.mu.Unlock()

	cancel = func() {
		b.unsubscribe(runID, subID)
	}

	return subID, subCh, cancel
}

func (b *LogBroker) unsubscribe(runID, subID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subsMap, ok := b.subscribers[runID]
	if !ok {
		return
	}

	sub, ok := subsMap[subID]
	if !ok {
		return
	}

	delete(subsMap, subID)
	if len(subsMap) == 0 {
		delete(b.subscribers, runID)
	}

	sub.closeOnce.Do(func() {
		close(sub.ch)
	})
}

// Publish broadcasts a chunk to all active subscribers of runID.
// It is non-blocking: slow subscribers will have the oldest chunk evicted (ring buffer style)
// or dropped if capacity is exceeded, guaranteeing worker producers are never blocked.
func (b *LogBroker) Publish(runID string, chunk []byte) {
	if len(chunk) == 0 || runID == "" {
		return
	}

	chunkCopy := make([]byte, len(chunk))
	copy(chunkCopy, chunk)

	b.mu.RLock()
	defer b.mu.RUnlock()

	subsMap, ok := b.subscribers[runID]
	if !ok || len(subsMap) == 0 {
		return
	}

	for _, sub := range subsMap {
		select {
		case sub.ch <- chunkCopy:
		default:
			// Ring buffer behavior: drop oldest chunk if channel is saturated
			select {
			case <-sub.ch:
			default:
			}
			// Attempt to enqueue fresh chunk
			select {
			case sub.ch <- chunkCopy:
			default:
				// Worker is never blocked even if channel remains full
			}
		}
	}
}

// CloseRun notifies and closes subscriber channels for runID, cleaning up memory.
func (b *LogBroker) CloseRun(runID string) {
	if runID == "" {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	subsMap, ok := b.subscribers[runID]
	if !ok {
		return
	}

	delete(b.subscribers, runID)

	for _, sub := range subsMap {
		sub.closeOnce.Do(func() {
			close(sub.ch)
		})
	}
}

// ActiveSubscribers returns the count of active subscribers for runID.
func (b *LogBroker) ActiveSubscribers(runID string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	subsMap, ok := b.subscribers[runID]
	if !ok {
		return 0
	}
	return len(subsMap)
}
