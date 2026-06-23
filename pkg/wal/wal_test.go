package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWALBasicReadWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-wal-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := NewWAL(tmpDir, true, 0, 1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write records
	key1 := []byte("key1")
	val1 := []byte("value1")
	o1, err := w.Append(key1, val1)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if o1 != 0 {
		t.Errorf("expected offset 0, got %d", o1)
	}

	key2 := []byte("key2")
	val2 := []byte("value2")
	o2, err := w.Append(key2, val2)
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}
	if o2 != 1 {
		t.Errorf("expected offset 1, got %d", o2)
	}

	// Read records
	r1, err := w.Read(0)
	if err != nil {
		t.Fatalf("failed to read 0: %v", err)
	}
	if !bytes.Equal(r1.Key, key1) || !bytes.Equal(r1.Value, val1) {
		t.Errorf("r1 mismatch: key=%s, val=%s", r1.Key, r1.Value)
	}

	r2, err := w.Read(1)
	if err != nil {
		t.Fatalf("failed to read 1: %v", err)
	}
	if !bytes.Equal(r2.Key, key2) || !bytes.Equal(r2.Value, val2) {
		t.Errorf("r2 mismatch: key=%s, val=%s", r2.Key, r2.Value)
	}

	_, err = w.Read(2)
	if err != ErrOffsetNotFound {
		t.Errorf("expected ErrOffsetNotFound, got %v", err)
	}
}

func TestWALSegmentRoll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-wal-roll-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Set small maxSegmentBytes to force rollover
	w, err := NewWAL(tmpDir, true, 0, 40)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	key := []byte("k")
	val := []byte("v")

	// Append a few times. Record size: 1 + 8 + 8 + 4 + 1 + 4 + 1 + 4 = 31 bytes.
	// 2 records will exceed 40 bytes, causing rollover.
	o1, err := w.Append(key, val)
	if err != nil {
		t.Fatalf("failed to append 1: %v", err)
	}
	o2, err := w.Append(key, val)
	if err != nil {
		t.Fatalf("failed to append 2: %v", err)
	}
	o3, err := w.Append(key, val)
	if err != nil {
		t.Fatalf("failed to append 3: %v", err)
	}

	if o1 != 0 || o2 != 1 || o3 != 2 {
		t.Errorf("unexpected offsets: %d, %d, %d", o1, o2, o3)
	}

	w.Close()

	// Check directory files
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	var logs []string
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".log" {
			logs = append(logs, f.Name())
		}
	}

	// Should have rolled and created multiple log files
	if len(logs) < 2 {
		t.Errorf("expected at least 2 log files, got %v", logs)
	}

	// Reopen WAL and verify we can read all records
	w2, err := NewWAL(tmpDir, true, 0, 40)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	for o := uint64(0); o < 3; o++ {
		r, err := w2.Read(o)
		if err != nil {
			t.Errorf("failed to read offset %d after reload: %v", o, err)
		} else if !bytes.Equal(r.Key, key) || !bytes.Equal(r.Value, val) {
			t.Errorf("mismatch at offset %d: key=%s, val=%s", o, r.Key, r.Value)
		}
	}

	if w2.NextOffset() != 3 {
		t.Errorf("expected NextOffset to be 3, got %d", w2.NextOffset())
	}
}

func TestWALRebuildIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-wal-rebuild-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := NewWAL(tmpDir, true, 0, 1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	key := []byte("key")
	val := []byte("val")
	w.Append(key, val)
	w.Append(key, val)
	w.Close()

	// Corrupt index files by deleting them
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	for _, f := range files {
		if filepath.Ext(f.Name()) == ".index" {
			err = os.Remove(filepath.Join(tmpDir, f.Name()))
			if err != nil {
				t.Fatalf("failed to delete index file: %v", err)
			}
		}
	}

	// Reopen, WAL should automatically rebuild indexes from log files
	w2, err := NewWAL(tmpDir, true, 0, 1024)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	r1, err := w2.Read(0)
	if err != nil {
		t.Fatalf("failed to read 0 after index rebuild: %v", err)
	}
	if !bytes.Equal(r1.Key, key) {
		t.Errorf("expected key %s, got %s", key, r1.Key)
	}

	r2, err := w2.Read(1)
	if err != nil {
		t.Fatalf("failed to read 1 after index rebuild: %v", err)
	}
	if !bytes.Equal(r2.Key, key) {
		t.Errorf("expected key %s, got %s", key, r2.Key)
	}

	if w2.NextOffset() != 2 {
		t.Errorf("expected NextOffset 2, got %d", w2.NextOffset())
	}
}

func TestWALAsyncFlush(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tumbleweed-wal-async-test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	w, err := NewWAL(tmpDir, false, 50*time.Millisecond, 1024)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	o, err := w.Append([]byte("async"), []byte("data"))
	if err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Wait for background flush loop
	time.Sleep(100 * time.Millisecond)

	r, err := w.Read(o)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}
	if string(r.Key) != "async" {
		t.Errorf("expected key 'async', got '%s'", r.Key)
	}
}
