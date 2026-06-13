package lsmtree

import (
	"fmt"
	"testing"
	"time"
)

func TestDBCompactionTrigger(t *testing.T) {
	db, err := Open(DBOptions{
		Dir:                 t.TempDir(),
		MemTableSize:        4096, // very small
		CompactionThreshold: 4,    // compact when 4 SSTables exist
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write enough entries to trigger multiple flushes, but not quite
	// reach the compaction threshold. We'll write enough for 3 SSTables.
	// Each 4KB MemTable holds ~40 entries.
	for i := 0; i < 120; i++ {
		db.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
	}
	db.waitForFlush()

	// Give the compaction goroutine a moment to ensure it doesn't do anything
	time.Sleep(50 * time.Millisecond)

	countBefore := db.SSTCount()
	if countBefore >= 4 {
		t.Fatalf("expected < 4 SSTables before threshold, got %d", countBefore)
	}
	t.Logf("SSTables before compaction: %d", countBefore)

	// Now write more to exceed the threshold (4 SSTables).
	for i := 120; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
	}
	db.waitForFlush()

	// Give compaction time to run (it's asynchronous).
	time.Sleep(100 * time.Millisecond)

	countAfter := db.SSTCount()
	if countAfter >= 4 {
		t.Fatalf("expected compaction to reduce SSTable count < 4, got %d", countAfter)
	}
	t.Logf("SSTables after compaction: %d", countAfter)

	// Verify all keys are still readable.
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k-%04d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		want := fmt.Sprintf("v-%04d", i)
		if string(val) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, val, want)
		}
	}
}

func TestDBCompactionPurgesTombstones(t *testing.T) {
	db, err := Open(DBOptions{
		Dir:                 t.TempDir(),
		MemTableSize:        4096,
		CompactionThreshold: 2, // aggressively compact
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Insert keys.
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%04d", i)))
	}
	// Delete half of them.
	for i := 0; i < 50; i++ {
		db.Delete([]byte(fmt.Sprintf("key-%04d", i)))
	}
	// Trigger flushes.
	for i := 100; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%04d", i)))
	}

	db.waitForFlush()
	time.Sleep(100 * time.Millisecond) // wait for compaction

	// Verify deleted keys are gone.
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		_, err := db.Get(key)
		if err != ErrKeyNotFound {
			t.Fatalf("deleted key %q should be missing, got err %v", key, err)
		}
	}

	// Verify live keys exist.
	for i := 50; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("live key %q missing: %v", key, err)
		}
		want := fmt.Sprintf("val-%04d", i)
		if string(val) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, val, want)
		}
	}
}

func TestDBCompactionShadowing(t *testing.T) {
	db, err := Open(DBOptions{
		Dir:                 t.TempDir(),
		MemTableSize:        4096,
		CompactionThreshold: 2, // compact quickly
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write old versions.
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("shadow-%04d", i)), []byte("old-value"))
	}
	db.waitForFlush()

	// Write new versions.
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("shadow-%04d", i)), []byte("new-value"))
	}
	db.waitForFlush()
	time.Sleep(100 * time.Millisecond) // wait for compaction

	// Read them back — we should only see "new-value".
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("shadow-%04d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error: %v", key, err)
		}
		if string(val) != "new-value" {
			t.Fatalf("Get(%q) = %q, want 'new-value' (shadowing failed)", key, val)
		}
	}
}
