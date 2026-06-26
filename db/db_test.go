package db

import (
	"fmt"
	"sync"
	"testing"
)

// Helpers

func openTestDB(t testing.TB) *DB {
	t.Helper()
	db, err := Open(DBOptions{
		Dir:          t.TempDir(),
		MemTableSize: 4 * 1024, // 4 KB — tiny threshold for fast rotation
		SyncOnWrite:  false,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return db
}

// Unit tests

func TestDBPutGet(t *testing.T) {
	db := openTestDB(t)

	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	val, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "world" {
		t.Fatalf("Get = %q, want %q", val, "world")
	}
}

func TestDBGetMissing(t *testing.T) {
	db := openTestDB(t)

	_, err := db.Get([]byte("nope"))
	if err != ErrKeyNotFound {
		t.Fatalf("Get missing: err = %v, want ErrKeyNotFound", err)
	}
}

func TestDBOverwrite(t *testing.T) {
	db := openTestDB(t)

	db.Put([]byte("k"), []byte("v1"))
	db.Put([]byte("k"), []byte("v2"))

	val, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "v2" {
		t.Fatalf("Get = %q, want %q", val, "v2")
	}
}

func TestDBDelete(t *testing.T) {
	db := openTestDB(t)

	db.Put([]byte("k"), []byte("v"))
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}

	_, err := db.Get([]byte("k"))
	if err != ErrKeyNotFound {
		t.Fatalf("Get after delete: err = %v, want ErrKeyNotFound", err)
	}
}

func TestDBDeleteShadowsSSTable(t *testing.T) {
	db := openTestDB(t)

	// Write enough data to force a flush to SSTable.
	for i := 0; i < 200; i++ {
		db.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%04d", i)))
	}
	db.waitForFlush()

	if db.SSTCount() == 0 {
		t.Fatal("expected at least 1 SSTable after bulk writes")
	}

	// Delete a key that's now in an SSTable.
	db.Delete([]byte("key-0050"))

	_, err := db.Get([]byte("key-0050"))
	if err != ErrKeyNotFound {
		t.Fatalf("tombstone should shadow SSTable: err = %v", err)
	}

	// Other keys should still be accessible.
	val, err := db.Get([]byte("key-0100"))
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "val-0100" {
		t.Fatalf("Get = %q, want %q", val, "val-0100")
	}
}

func TestDBFlushToSSTable(t *testing.T) {
	db := openTestDB(t)

	// With 4 KB threshold, ~40 entries should trigger a flush.
	for i := 0; i < 200; i++ {
		if err := db.Put(
			[]byte(fmt.Sprintf("flush-key-%04d", i)),
			[]byte(fmt.Sprintf("flush-val-%04d", i)),
		); err != nil {
			t.Fatal(err)
		}
	}
	db.waitForFlush()

	if db.SSTCount() == 0 {
		t.Fatal("expected SSTables after exceeding MemTable threshold")
	}
	t.Logf("SSTCount = %d after 200 puts", db.SSTCount())

	// All keys should still be readable.
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("flush-key-%04d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		want := fmt.Sprintf("flush-val-%04d", i)
		if string(val) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, val, want)
		}
	}
}

func TestDBRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write data and close.
	{
		db, err := Open(DBOptions{Dir: dir, MemTableSize: 4096})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 50; i++ {
			db.Put([]byte(fmt.Sprintf("rec-%04d", i)), []byte(fmt.Sprintf("val-%04d", i)))
		}
		// Force a WAL sync so data is durable.
		db.wal.Sync()
		db.Close()
	}

	// Phase 2: reopen and verify all data survives.
	{
		db, err := Open(DBOptions{Dir: dir, MemTableSize: 4096})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("rec-%04d", i))
			val, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get(%q) after recovery: %v", key, err)
			}
			want := fmt.Sprintf("val-%04d", i)
			if string(val) != want {
				t.Fatalf("Get(%q) = %q, want %q after recovery", key, val, want)
			}
		}
	}
}

func TestDBRecoveryWithSSTables(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write enough to flush SSTables, then write more to WAL.
	{
		db, err := Open(DBOptions{Dir: dir, MemTableSize: 4096})
		if err != nil {
			t.Fatal(err)
		}
		// Bulk write to trigger SSTable flushes.
		for i := 0; i < 200; i++ {
			db.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
		}
		db.waitForFlush()
		t.Logf("SSTCount after phase 1: %d", db.SSTCount())

		// Write a few more (these stay in WAL + MemTable).
		for i := 200; i < 210; i++ {
			db.Put([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
		}
		db.wal.Sync()
		db.Close()
	}

	// Phase 2: reopen and verify everything.
	{
		db, err := Open(DBOptions{Dir: dir, MemTableSize: 4096})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		for i := 0; i < 210; i++ {
			key := []byte(fmt.Sprintf("k-%04d", i))
			_, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get(%q) after recovery: %v", key, err)
			}
		}
	}
}

func TestDBConcurrentReadWrite(t *testing.T) {
	db := openTestDB(t)

	const writers = 4
	const readers = 4
	const ops = 200

	var wg sync.WaitGroup

	// Writers.
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := []byte(fmt.Sprintf("w%d-k%04d", id, i))
				val := []byte(fmt.Sprintf("w%d-v%04d", id, i))
				if err := db.Put(key, val); err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}(w)
	}

	// Readers (may or may not find keys).
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := []byte(fmt.Sprintf("w%d-k%04d", id%writers, i))
				val, err := db.Get(key)
				if err == ErrKeyNotFound {
					continue // not written yet
				}
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				_ = val // value is valid if err == nil
			}
		}(r)
	}

	wg.Wait()
	db.waitForFlush()

	// Verify all written keys exist.
	for w := 0; w < writers; w++ {
		for i := 0; i < ops; i++ {
			key := []byte(fmt.Sprintf("w%d-k%04d", w, i))
			_, err := db.Get(key)
			if err != nil {
				t.Fatalf("Get(%q) after concurrent writes: %v", key, err)
			}
		}
	}
}

func TestDBSyncOnWrite(t *testing.T) {
	db, err := Open(DBOptions{
		Dir:          t.TempDir(),
		MemTableSize: 1 << 20, // 1 MB
		SyncOnWrite:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// With SyncOnWrite, each Put fsyncs. Just verify it doesn't error.
	for i := 0; i < 10; i++ {
		if err := db.Put([]byte(fmt.Sprintf("sync-%d", i)), []byte("val")); err != nil {
			t.Fatal(err)
		}
	}
	val, err := db.Get([]byte("sync-5"))
	if err != nil || string(val) != "val" {
		t.Fatalf("Get = %q, err = %v", val, err)
	}
}

func TestDBMultipleFlushes(t *testing.T) {
	db := openTestDB(t) // 4 KB threshold

	// Write enough to trigger multiple flushes.
	for i := 0; i < 1000; i++ {
		db.Put([]byte(fmt.Sprintf("multi-%06d", i)), []byte(fmt.Sprintf("val-%06d", i)))
	}
	db.waitForFlush()

	if db.SSTCount() < 2 {
		t.Fatalf("expected multiple SSTables, got %d", db.SSTCount())
	}
	t.Logf("SSTCount = %d after 1000 puts", db.SSTCount())

	// Spot-check some keys.
	for _, i := range []int{0, 100, 500, 999} {
		key := []byte(fmt.Sprintf("multi-%06d", i))
		val, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		want := fmt.Sprintf("val-%06d", i)
		if string(val) != want {
			t.Fatalf("Get(%q) = %q, want %q", key, val, want)
		}
	}
}

// Benchmarks

func BenchmarkDBPut(b *testing.B) {
	db, err := Open(DBOptions{
		Dir:          b.TempDir(),
		MemTableSize: 64 * 1024 * 1024, // 64 MB — avoid flush overhead
		SyncOnWrite:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	key := []byte("bench-key-0123456789")
	val := []byte("bench-val-0123456789-with-payload")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := db.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDBGet(b *testing.B) {
	db, err := Open(DBOptions{
		Dir:          b.TempDir(),
		MemTableSize: 64 * 1024 * 1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 10000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("bench-key-%08d", i))
		db.Put(keys[i], keys[i])
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i%n])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDBGetFromSSTable(b *testing.B) {
	db, err := Open(DBOptions{
		Dir:          b.TempDir(),
		MemTableSize: 4096, // tiny — forces flush
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	const n = 5000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("sst-key-%08d", i))
		db.Put(keys[i], keys[i])
	}
	db.waitForFlush()

	if db.SSTCount() == 0 {
		b.Fatal("expected SSTables")
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i%n])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDBPutParallel(b *testing.B) {
	db, err := Open(DBOptions{
		Dir:          b.TempDir(),
		MemTableSize: 64 * 1024 * 1024,
		SyncOnWrite:  false,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("par-%08d", i))
			db.Put(key, key)
			i++
		}
	})
}
