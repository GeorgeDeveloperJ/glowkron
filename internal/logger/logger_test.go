package logger_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/GeorgeDeveloperJ/glowkron/internal/logger"
)

func TestFileAppender_WriteAndRead(t *testing.T) {
	tempDir := t.TempDir()
	appender, err := logger.NewFileAppender(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileAppender: %v", err)
	}

	runID := "run-test-write-read"
	chunk1 := []byte("2026-10-13 18:00:00 [INFO] Step 1 starting\n")
	chunk2 := []byte("2026-10-13 18:00:01 [INFO] Step 1 executed successfully\n")
	chunk3 := []byte("2026-10-13 18:00:02 [INFO] Job completed\n")

	// 1. Direct Write
	n, err := appender.Write(runID, chunk1)
	if err != nil {
		t.Fatalf("Write chunk1 error: %v", err)
	}
	if n != len(chunk1) {
		t.Errorf("expected %d bytes written, got %d", len(chunk1), n)
	}

	// 2. OpenWriter
	writer, err := appender.OpenWriter(runID)
	if err != nil {
		t.Fatalf("OpenWriter error: %v", err)
	}
	if _, err := writer.Write(chunk2); err != nil {
		t.Fatalf("writer.Write chunk2 error: %v", err)
	}
	if _, err := writer.Write(chunk3); err != nil {
		t.Fatalf("writer.Write chunk3 error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close error: %v", err)
	}

	// 3. Read full logs
	fullLogs, err := appender.ReadLogs(runID)
	if err != nil {
		t.Fatalf("ReadLogs error: %v", err)
	}

	expectedFull := append(append([]byte{}, chunk1...), append(chunk2, chunk3...)...)
	if !bytes.Equal(fullLogs, expectedFull) {
		t.Errorf("full logs mismatch:\ngot:\n%s\nwant:\n%s", fullLogs, expectedFull)
	}

	// 4. Read tail with smaller maxBytes (chunk3 only)
	tailChunk3, err := appender.ReadLogsTail(runID, int64(len(chunk3)))
	if err != nil {
		t.Fatalf("ReadLogsTail error: %v", err)
	}
	if !bytes.Equal(tailChunk3, chunk3) {
		t.Errorf("tail chunk3 mismatch: got %q, want %q", tailChunk3, chunk3)
	}

	// 5. Read tail with maxBytes larger than total file size
	tailAll, err := appender.ReadLogsTail(runID, int64(len(expectedFull)*2))
	if err != nil {
		t.Fatalf("ReadLogsTail large error: %v", err)
	}
	if !bytes.Equal(tailAll, expectedFull) {
		t.Errorf("tail all mismatch: got %q, want %q", tailAll, expectedFull)
	}

	// 6. Read tail with maxBytes == 0
	tailEmpty, err := appender.ReadLogsTail(runID, 0)
	if err != nil {
		t.Fatalf("ReadLogsTail zero error: %v", err)
	}
	if len(tailEmpty) != 0 {
		t.Errorf("expected empty tail for maxBytes 0, got len %d", len(tailEmpty))
	}
}

func TestFileAppender_PathSafety(t *testing.T) {
	tempDir := t.TempDir()
	appender, err := logger.NewFileAppender(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileAppender: %v", err)
	}

	badRunIDs := []string{
		"../escape",
		"sub/dir",
		"sub\\dir",
		"..",
		"",
	}

	for _, badID := range badRunIDs {
		_, err := appender.Write(badID, []byte("danger"))
		if err == nil {
			t.Errorf("expected error for bad runID %q, got nil", badID)
		}
	}
}

func TestFileAppender_ConcurrentWrites(t *testing.T) {
	tempDir := t.TempDir()
	appender, err := logger.NewFileAppender(tempDir)
	if err != nil {
		t.Fatalf("failed to create FileAppender: %v", err)
	}

	runID := "run-concurrent"
	const goroutines = 20
	const linesPerRoutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < linesPerRoutine; j++ {
				msg := []byte(fmt.Sprintf("routine-%02d line-%04d\n", routineID, j))
				if _, err := appender.Write(runID, msg); err != nil {
					t.Errorf("concurrent write error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	data, err := appender.ReadLogs(runID)
	if err != nil {
		t.Fatalf("ReadLogs error: %v", err)
	}

	expectedBytes := goroutines * linesPerRoutine * len("routine-00 line-0000\n")
	if len(data) != expectedBytes {
		t.Errorf("expected %d total bytes, got %d", expectedBytes, len(data))
	}
}

func TestPubSubBroker_Broadcast(t *testing.T) {
	broker := logger.NewLogBroker()
	runID := "run-broadcast"

	sub1ID, ch1, cancel1 := broker.Subscribe(runID)
	defer cancel1()

	sub2ID, ch2, cancel2 := broker.Subscribe(runID)
	defer cancel2()

	if sub1ID == sub2ID {
		t.Errorf("expected unique subscriber IDs, got collision: %s", sub1ID)
	}

	if active := broker.ActiveSubscribers(runID); active != 2 {
		t.Fatalf("expected 2 active subscribers, got %d", active)
	}

	testChunks := [][]byte{
		[]byte("log message 1"),
		[]byte("log message 2"),
		[]byte("log message 3"),
	}

	for _, chunk := range testChunks {
		broker.Publish(runID, chunk)
	}

	// Verify subscriber 1
	for _, want := range testChunks {
		select {
		case got := <-ch1:
			if !bytes.Equal(got, want) {
				t.Errorf("ch1 received %q, want %q", got, want)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for chunk on ch1")
		}
	}

	// Verify subscriber 2
	for _, want := range testChunks {
		select {
		case got := <-ch2:
			if !bytes.Equal(got, want) {
				t.Errorf("ch2 received %q, want %q", got, want)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for chunk on ch2")
		}
	}

	cancel1()
	if active := broker.ActiveSubscribers(runID); active != 1 {
		t.Errorf("expected 1 active subscriber after cancel1, got %d", active)
	}
}

func TestPubSubBroker_SlowSubscriberNonBlocking(t *testing.T) {
	// Small buffer capacity of 2
	broker := logger.NewLogBroker(2)
	runID := "run-slow-reader"

	// Slow subscriber (never reads until end)
	_, slowCh, cancelSlow := broker.Subscribe(runID)
	defer cancelSlow()

	// Fast subscriber
	_, fastCh, cancelFast := broker.Subscribe(runID)
	defer cancelFast()

	doneCh := make(chan struct{})
	go func() {
		// Publish 20 chunks rapidly. Slow subscriber has buffer size 2.
		for i := 0; i < 20; i++ {
			broker.Publish(runID, []byte(fmt.Sprintf("chunk-%d", i)))
		}
		close(doneCh)
	}()

	select {
	case <-doneCh:
		// Succeeded without deadlock or blocking
	case <-time.After(2 * time.Second):
		t.Fatalf("broker.Publish deadlocked on slow subscriber!")
	}

	// Fast subscriber should have received chunks
	fastCount := 0
	readDone := false
	for !readDone {
		select {
		case <-fastCh:
			fastCount++
		default:
			readDone = true
		}
	}

	if fastCount == 0 {
		t.Errorf("fast subscriber received zero chunks")
	}

	// Slow subscriber buffer should be bounded by bufferSize (2)
	slowCount := len(slowCh)
	if slowCount > 2 {
		t.Errorf("slow subscriber buffer exceeded capacity: got %d, max 2", slowCount)
	}
}

func TestPubSubBroker_CloseRun(t *testing.T) {
	broker := logger.NewLogBroker()
	runID := "run-close"

	_, ch, _ := broker.Subscribe(runID)

	broker.Publish(runID, []byte("final message"))
	broker.CloseRun(runID)

	// Subscriber should read remaining chunk, then receive closed channel
	select {
	case chunk, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed prematurely before receiving final message")
		}
		if string(chunk) != "final message" {
			t.Errorf("unexpected chunk: %s", string(chunk))
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for final message")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Errorf("expected channel to be closed, but channel was still open")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for channel close")
	}

	if active := broker.ActiveSubscribers(runID); active != 0 {
		t.Errorf("expected 0 active subscribers after CloseRun, got %d", active)
	}
}

func TestPubSubBroker_IdempotentCancelAndClose(t *testing.T) {
	broker := logger.NewLogBroker()
	runID := "run-idempotent"

	_, ch, cancel := broker.Subscribe(runID)

	// Call cancel multiple times; must not panic
	cancel()
	cancel()

	// Call CloseRun; must not panic
	broker.CloseRun(runID)
	broker.CloseRun(runID)

	// Channel must be closed
	_, ok := <-ch
	if ok {
		t.Errorf("expected channel to be closed")
	}
}
