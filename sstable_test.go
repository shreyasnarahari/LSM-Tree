package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// Bloom filter tests
// ---------------------------------------------------------------------------

func TestBloomFilterBasic(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	for i := 0; i < 1000; i++ {
		bf.Add([]byte(fmt.Sprintf("key-%04d", i)))
	}

	// All inserted keys must be found (no false negatives).
	for i := 0; i < 1000; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key-%04d", i))) {
			t.Fatalf("false negative for key-%04d", i)
		}
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	const n = 100_000
	bf := NewBloomFilter(n, 0.01)
	for i := 0; i < n; i++ {
		bf.Add([]byte(fmt.Sprintf("bloom-%08d", i)))
	}

	// Probe keys that were never inserted.
	falsePositives := 0
	const probes = 100_000
	for i := 0; i < probes; i++ {
		if bf.MayContain([]byte(fmt.Sprintf("absent-%08d", i))) {
			falsePositives++
		}
	}
	rate := float64(falsePositives) / float64(probes)
	t.Logf("false positive rate: %.4f%% (%d / %d)", rate*100, falsePositives, probes)
	// Allow up to 2% (generous margin over target 1%).
	if rate > 0.02 {
		t.Fatalf("false positive rate %.4f exceeds 2%%", rate)
	}
}

func TestBloomFilterSerialization(t *testing.T) {
	bf := NewBloomFilter(500, 0.01)
	for i := 0; i < 500; i++ {
		bf.Add([]byte(fmt.Sprintf("ser-%04d", i)))
	}

	data := bf.MarshalBinary()
	bf2 := UnmarshalBloomFilter(data)

	// All keys must survive the roundtrip.
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("ser-%04d", i))
		if !bf2.MayContain(key) {
			t.Fatalf("false negative after deserialization for key %d", i)
		}
	}
	if bf2.numBits != bf.numBits || bf2.numHashes != bf.numHashes {
		t.Fatalf("params mismatch: got (%d,%d), want (%d,%d)",
			bf2.numBits, bf2.numHashes, bf.numBits, bf.numHashes)
	}
}

// ---------------------------------------------------------------------------
// SSTable build + read round-trip tests
// ---------------------------------------------------------------------------

// buildTestSSTable populates a MemTable with n entries and flushes it to an SSTable.
func buildTestSSTable(t testing.TB, dir string, name string, n int) string {
	t.Helper()
	mt := NewMemTable(1 << 30)
	for i := 0; i < n; i++ {
		mt.PutWithTimestamp(
			[]byte(fmt.Sprintf("key-%06d", i)),
			[]byte(fmt.Sprintf("value-%06d", i)),
			uint64(i+1), false,
		)
	}
	path := filepath.Join(dir, name)
	if err := BuildSSTable(path, mt.Iterator(), mt.Len()); err != nil {
		t.Fatalf("BuildSSTable: %v", err)
	}
	return path
}

func TestSSTableRoundTrip(t *testing.T) {
	dir := t.TempDir()
	const n = 500
	path := buildTestSSTable(t, dir, "test.sst", n)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable: %v", err)
	}
	defer reader.Close()

	// Every inserted key must be found.
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		wantVal := fmt.Sprintf("value-%06d", i)
		val, found, tomb, err := reader.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !found {
			t.Fatalf("Get(%q): not found", key)
		}
		if tomb {
			t.Fatalf("Get(%q): unexpected tombstone", key)
		}
		if string(val) != wantVal {
			t.Fatalf("Get(%q) = %q, want %q", key, val, wantVal)
		}
	}

	// Keys not in the SSTable must not be found.
	for i := n; i < n+100; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		_, found, _, err := reader.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if found {
			t.Fatalf("Get(%q): should not be found", key)
		}
	}
}

func TestSSTableTombstone(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(1 << 30)
	mt.PutWithTimestamp([]byte("alive"), []byte("value"), 1, false)
	mt.PutWithTimestamp([]byte("dead"), nil, 2, true)

	path := filepath.Join(dir, "tomb.sst")
	if err := BuildSSTable(path, mt.Iterator(), mt.Len()); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	val, found, tomb, _ := reader.Get([]byte("alive"))
	if !found || tomb || string(val) != "value" {
		t.Fatalf("alive: found=%v tomb=%v val=%q", found, tomb, val)
	}

	val, found, tomb, _ = reader.Get([]byte("dead"))
	if !found || !tomb {
		t.Fatalf("dead: found=%v tomb=%v", found, tomb)
	}
	if val != nil {
		t.Fatalf("tombstone value should be nil, got %q", val)
	}
}

func TestSSTableSingleBlockRead(t *testing.T) {
	// With small entries, many fit in one block. Verify we only read one
	// block per Get by checking the block count in the reader.
	dir := t.TempDir()
	const n = 5000
	path := buildTestSSTable(t, dir, "blocks.sst", n)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	t.Logf("SSTable has %d data blocks for %d entries", reader.BlockCount(), n)
	if reader.BlockCount() < 2 {
		t.Fatal("expected multiple data blocks for 5000 entries")
	}

	// Verify a few random lookups still succeed.
	for _, i := range []int{0, 100, 2500, 4999} {
		key := []byte(fmt.Sprintf("key-%06d", i))
		_, found, _, err := reader.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("key-%06d not found", i)
		}
	}
}

func TestSSTableBloomFilterRejectsAbsentKeys(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	path := buildTestSSTable(t, dir, "bloom-reject.sst", n)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// Probe absent keys — bloom should reject most without disk I/O.
	rejected := 0
	const probes = 1000
	for i := 0; i < probes; i++ {
		key := []byte(fmt.Sprintf("absent-%08d", i))
		if !reader.bloom.MayContain(key) {
			rejected++
		}
	}
	rejectRate := float64(rejected) / float64(probes)
	t.Logf("bloom rejected %d/%d absent keys (%.1f%%)", rejected, probes, rejectRate*100)
	if rejectRate < 0.95 {
		t.Fatalf("bloom should reject >95%% of absent keys, got %.1f%%", rejectRate*100)
	}
}

func TestSSTableMagicNumberValidation(t *testing.T) {
	dir := t.TempDir()
	path := buildTestSSTable(t, dir, "magic.sst", 10)

	// Corrupt the magic number (last 8 bytes of file).
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	if _, err := f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, fi.Size()-8); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = OpenSSTable(path)
	if err == nil {
		t.Fatal("expected error for bad magic number")
	}
}

func TestSSTableEmpty(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(1 << 30)
	path := filepath.Join(dir, "empty.sst")
	if err := BuildSSTable(path, mt.Iterator(), 0); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	_, found, _, err := reader.Get([]byte("anything"))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("empty SSTable should never find a key")
	}
}

func TestSSTableLargeValues(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(1 << 30)
	bigVal := make([]byte, 2048) // 2 KB value — ensures few entries per block
	for i := range bigVal {
		bigVal[i] = byte(i % 256)
	}
	for i := 0; i < 50; i++ {
		mt.PutWithTimestamp([]byte(fmt.Sprintf("big-%04d", i)), bigVal, uint64(i+1), false)
	}

	path := filepath.Join(dir, "large.sst")
	if err := BuildSSTable(path, mt.Iterator(), mt.Len()); err != nil {
		t.Fatal(err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	for i := 0; i < 50; i++ {
		val, found, _, err := reader.Get([]byte(fmt.Sprintf("big-%04d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			t.Fatalf("big-%04d not found", i)
		}
		if len(val) != 2048 {
			t.Fatalf("big-%04d: value len=%d, want 2048", i, len(val))
		}
	}
}

func TestSSTableMinKey(t *testing.T) {
	dir := t.TempDir()
	path := buildTestSSTable(t, dir, "minkey.sst", 100)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	if string(reader.MinKey()) != "key-000000" {
		t.Fatalf("MinKey = %q, want %q", reader.MinKey(), "key-000000")
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(b.N, 0.01)
	key := []byte("benchmark-bloom-key-12345")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

func BenchmarkBloomFilterMayContain(b *testing.B) {
	const n = 100_000
	bf := NewBloomFilter(n, 0.01)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("bloom-bench-%08d", i))
		bf.Add(keys[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bf.MayContain(keys[i%n])
	}
}

func BenchmarkSSTableBuild(b *testing.B) {
	dir := b.TempDir()
	const n = 10_000

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mt := NewMemTable(1 << 30)
		for j := 0; j < n; j++ {
			mt.PutWithTimestamp(
				[]byte(fmt.Sprintf("key-%08d", j)),
				[]byte(fmt.Sprintf("val-%08d", j)),
				uint64(j), false,
			)
		}
		path := filepath.Join(dir, fmt.Sprintf("bench-%d.sst", i))
		if err := BuildSSTable(path, mt.Iterator(), n); err != nil {
			b.Fatal(err)
		}
		os.Remove(path)
	}
}

func BenchmarkSSTableGet(b *testing.B) {
	dir := b.TempDir()
	const n = 50_000

	mt := NewMemTable(1 << 30)
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		mt.PutWithTimestamp(keys[i], keys[i], uint64(i), false)
	}
	path := filepath.Join(dir, "get-bench.sst")
	if err := BuildSSTable(path, mt.Iterator(), n); err != nil {
		b.Fatal(err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, err := reader.Get(keys[i%n])
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSSTableGetMiss(b *testing.B) {
	dir := b.TempDir()
	const n = 50_000

	mt := NewMemTable(1 << 30)
	for i := 0; i < n; i++ {
		mt.PutWithTimestamp(
			[]byte(fmt.Sprintf("key-%08d", i)),
			[]byte(fmt.Sprintf("val-%08d", i)),
			uint64(i), false,
		)
	}
	path := filepath.Join(dir, "miss-bench.sst")
	if err := BuildSSTable(path, mt.Iterator(), n); err != nil {
		b.Fatal(err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		b.Fatal(err)
	}
	defer reader.Close()

	missKeys := make([][]byte, 10000)
	for i := range missKeys {
		missKeys[i] = []byte(fmt.Sprintf("miss-%08d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _, _ = reader.Get(missKeys[i%len(missKeys)])
	}
}
