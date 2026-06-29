package memtable

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"testing"
)

// Skip List — internal correctness tests

func TestRBTreePutGet(t *testing.T) {
	sl := newRedBlackTree()
	sl.Put([]byte("apple"), []byte("red"), 1, false)
	sl.Put([]byte("banana"), []byte("yellow"), 2, false)
	sl.Put([]byte("cherry"), []byte("dark"), 3, false)

	tests := []struct {
		key     string
		wantVal string
		wantOK  bool
	}{
		{"apple", "red", true},
		{"banana", "yellow", true},
		{"cherry", "dark", true},
		{"durian", "", false},
	}
	for _, tt := range tests {
		val, _, ok, _ := sl.Get([]byte(tt.key))
		if ok != tt.wantOK {
			t.Errorf("Get(%q) found=%v, want %v", tt.key, ok, tt.wantOK)
		}
		if ok && string(val) != tt.wantVal {
			t.Errorf("Get(%q) = %q, want %q", tt.key, val, tt.wantVal)
		}
	}
}

func TestRBTreeOverwrite(t *testing.T) {
	sl := newRedBlackTree()
	sl.Put([]byte("k"), []byte("v1"), 1, false)
	sl.Put([]byte("k"), []byte("v2-longer"), 2, false)

	val, ts, ok, _ := sl.Get([]byte("k"))
	if !ok || string(val) != "v2-longer" || ts != 2 {
		t.Fatalf("after overwrite: val=%q ts=%d ok=%v", val, ts, ok)
	}
	if sl.Len() != 1 {
		t.Fatalf("length should be 1 after overwrite, got %d", sl.Len())
	}
}

func TestRBTreeTombstone(t *testing.T) {
	sl := newRedBlackTree()
	sl.Put([]byte("k"), []byte("val"), 1, false)
	sl.Put([]byte("k"), nil, 2, true) // delete

	val, _, ok, tomb := sl.Get([]byte("k"))
	if !ok {
		t.Fatal("tombstoned key should still be found")
	}
	if !tomb {
		t.Fatal("expected tombstone=true")
	}
	if val != nil {
		t.Fatalf("tombstone value should be nil, got %q", val)
	}
}

func TestRBTreeOrdering(t *testing.T) {
	sl := newRedBlackTree()
	keys := []string{"zebra", "mango", "apple", "cherry", "banana"}
	for i, k := range keys {
		sl.Put([]byte(k), []byte(k), uint64(i), false)
	}

	// Walk the tree and collect keys.
	var got []string
	for n := sl.front(); n != nil; n = successor(n) {
		got = append(got, string(n.key))
	}

	sort.Strings(keys)
	if len(got) != len(keys) {
		t.Fatalf("got %d keys, want %d", len(got), len(keys))
	}
	for i := range keys {
		if got[i] != keys[i] {
			t.Errorf("position %d: got %q, want %q", i, got[i], keys[i])
		}
	}
}

func TestRBTreeSizeTracking(t *testing.T) {
	sl := newRedBlackTree()
	if sl.Size() != 0 {
		t.Fatalf("empty size = %d, want 0", sl.Size())
	}

	sl.Put([]byte("key"), []byte("val"), 1, false)
	s1 := sl.Size()
	if s1 <= 0 {
		t.Fatalf("size after insert should be > 0, got %d", s1)
	}

	// Overwrite with a longer value: size should increase.
	sl.Put([]byte("key"), []byte("longer-value"), 2, false)
	s2 := sl.Size()
	if s2 <= s1 {
		t.Fatalf("size should increase after longer value: %d <= %d", s2, s1)
	}

	// Overwrite with shorter value: size should decrease.
	sl.Put([]byte("key"), []byte("s"), 3, false)
	s3 := sl.Size()
	if s3 >= s2 {
		t.Fatalf("size should decrease after shorter value: %d >= %d", s3, s2)
	}
}

// MemTable — public API tests

func TestMemTablePutGet(t *testing.T) {
	mt := New(4 * 1024 * 1024) // 4 MB threshold
	mt.Put([]byte("hello"), []byte("world"))

	val, found, deleted := mt.Get([]byte("hello"))
	if !found {
		t.Fatal("key not found")
	}
	if deleted {
		t.Fatal("unexpected tombstone")
	}
	if string(val) != "world" {
		t.Fatalf("value = %q, want %q", val, "world")
	}

	// Missing key.
	_, found, _ = mt.Get([]byte("missing"))
	if found {
		t.Fatal("missing key should not be found")
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := New(4 * 1024 * 1024)
	mt.Put([]byte("k"), []byte("v"))
	mt.Delete([]byte("k"))

	val, found, deleted := mt.Get([]byte("k"))
	if !found {
		t.Fatal("tombstoned key should be found")
	}
	if !deleted {
		t.Fatal("expected isDeleted=true")
	}
	if val != nil {
		t.Fatalf("deleted value should be nil, got %q", val)
	}
}

func TestMemTableIsFull(t *testing.T) {
	const threshold = 1024 // tiny threshold for testing
	mt := New(threshold)

	// Insert enough data to exceed threshold.
	for i := 0; !mt.IsFull(); i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		mt.Put(key, val)
		if i > 10000 {
			t.Fatal("MemTable never reached threshold")
		}
	}

	if mt.Size() < threshold {
		t.Fatalf("size %d should be >= threshold %d", mt.Size(), threshold)
	}
}

func TestMemTablePutWithTimestamp(t *testing.T) {
	mt := New(4 * 1024 * 1024)
	mt.PutWithTimestamp([]byte("k"), []byte("v"), 42, false)
	mt.PutWithTimestamp([]byte("d"), nil, 43, true)

	// Verify via internal skip list that timestamps are preserved.
	_, ts, ok, _ := mt.tree.Get([]byte("k"))
	if !ok || ts != 42 {
		t.Fatalf("expected ts=42, got ts=%d ok=%v", ts, ok)
	}
	_, ts, ok, tomb := mt.tree.Get([]byte("d"))
	if !ok || ts != 43 || !tomb {
		t.Fatalf("expected ts=43 tomb=true, got ts=%d tomb=%v ok=%v", ts, tomb, ok)
	}
}

func TestMemTableIterator(t *testing.T) {
	mt := New(4 * 1024 * 1024)
	input := []string{"delta", "alpha", "charlie", "bravo", "echo"}
	for i, k := range input {
		mt.PutWithTimestamp([]byte(k), []byte(k+"-val"), uint64(i+1), false)
	}

	// Add a tombstone.
	mt.PutWithTimestamp([]byte("foxtrot"), nil, 100, true)

	// Iterator should yield all entries in sorted order.
	sorted := append([]string{}, input...)
	sorted = append(sorted, "foxtrot")
	sort.Strings(sorted)

	it := mt.NewIterator()
	var got []string
	for it.Valid() {
		got = append(got, string(it.Key()))
		it.Next()
	}

	if len(got) != len(sorted) {
		t.Fatalf("iterator yielded %d entries, want %d", len(got), len(sorted))
	}
	for i := range sorted {
		if got[i] != sorted[i] {
			t.Errorf("pos %d: got %q, want %q", i, got[i], sorted[i])
		}
	}
}

func TestMemTableIteratorTombstone(t *testing.T) {
	mt := New(4 * 1024 * 1024)
	mt.PutWithTimestamp([]byte("a"), []byte("alive"), 1, false)
	mt.PutWithTimestamp([]byte("b"), nil, 2, true)

	it := mt.NewIterator()
	if !it.Valid() || string(it.Key()) != "a" || it.Tombstone() {
		t.Fatal("first entry should be 'a', not tombstone")
	}
	it.Next()
	if !it.Valid() || string(it.Key()) != "b" || !it.Tombstone() {
		t.Fatal("second entry should be 'b', tombstone")
	}
	it.Next()
	if it.Valid() {
		t.Fatal("iterator should be exhausted")
	}
}

func TestMemTableKeyCopySafety(t *testing.T) {
	mt := New(4 * 1024 * 1024)
	key := []byte("mutable")
	val := []byte("data")
	mt.Put(key, val)

	// Mutate the original slices — MemTable should be unaffected.
	key[0] = 'X'
	val[0] = 'X'

	v, found, _ := mt.Get([]byte("mutable"))
	if !found {
		t.Fatal("key should still be found after caller mutation")
	}
	if string(v) != "data" {
		t.Fatalf("value corrupted by caller mutation: got %q", v)
	}
}

// Concurrency tests

func TestRBTreeConcurrentReads(t *testing.T) {
	sl := newRedBlackTree()
	const n = 1000
	for i := 0; i < n; i++ {
		sl.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i)), uint64(i), false)
	}

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("k%04d", i))
				val, _, ok, _ := sl.Get(key)
				if !ok {
					t.Errorf("key %q not found", key)
					return
				}
				want := fmt.Sprintf("v%04d", i)
				if string(val) != want {
					t.Errorf("Get(%q) = %q, want %q", key, val, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestRBTreeConcurrentWriteRead(t *testing.T) {
	sl := newRedBlackTree()
	const n = 500

	var wg sync.WaitGroup

	// Writer goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("k%04d", i))
			sl.Put(key, key, uint64(i), false)
		}
	}()

	// Reader goroutines — may or may not find keys depending on timing.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				key := []byte(fmt.Sprintf("k%04d", i))
				val, _, ok, _ := sl.Get(key)
				if ok && !bytes.Equal(val, key) {
					t.Errorf("corrupt read: Get(%q) = %q", key, val)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Benchmarks

func BenchmarkRBTreePut(b *testing.B) {
	sl := newRedBlackTree()
	keys := make([][]byte, 10000)
	vals := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("bench-key-%08d", i))
		vals[i] = []byte(fmt.Sprintf("bench-val-%08d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := i % len(keys)
		sl.Put(keys[idx], vals[idx], uint64(i), false)
	}
}

func BenchmarkRBTreeGet(b *testing.B) {
	sl := newRedBlackTree()
	const n = 10000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("bench-key-%08d", i))
		sl.Put(keys[i], keys[i], uint64(i), false)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sl.Get(keys[i%n])
	}
}

func BenchmarkRBTreePutParallel(b *testing.B) {
	sl := newRedBlackTree()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("par-key-%08d", i))
			sl.Put(key, key, uint64(i), false)
			i++
		}
	})
}

func BenchmarkRBTreeGetParallel(b *testing.B) {
	sl := newRedBlackTree()
	const n = 10000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("par-key-%08d", i))
		sl.Put(keys[i], keys[i], uint64(i), false)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sl.Get(keys[i%n])
			i++
		}
	})
}

func BenchmarkMemTablePut(b *testing.B) {
	mt := New(1 << 30) // 1 GB — won't fill
	key := []byte("memtable-bench-key")
	val := []byte("memtable-bench-value-with-realistic-size")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mt.Put(key, val)
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	mt := New(1 << 30)
	const n = 10000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("mt-key-%08d", i))
		mt.Put(keys[i], keys[i])
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mt.Get(keys[i%n])
	}
}
