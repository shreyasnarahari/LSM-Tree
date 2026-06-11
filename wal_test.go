package lsmtree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// walPath returns a fresh WAL file path inside t.TempDir().
func walPath(t testing.TB) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.wal")
}

// appendEntries writes n entries to w with deterministic key/value patterns.
func appendEntries(t testing.TB, w *WAL, n int, tombstone bool) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		if err := w.Append(key, val, tombstone); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
}

// replayAll reads every valid entry from the WAL at path.
func replayAll(t testing.TB, path string) []*WALEntry {
	t.Helper()
	it, err := NewWALIterator(path)
	if err != nil {
		t.Fatalf("NewWALIterator: %v", err)
	}
	defer it.Close()

	var entries []*WALEntry
	for {
		e, err := it.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if e == nil {
			break
		}
		entries = append(entries, e)
	}
	return entries
}

// fileSize returns the byte length of path.
func fileSize(t testing.TB, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	return info.Size()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

// TestWALAppendAndReplay writes N entries, closes, replays, and verifies
// every key/value/timestamp roundtrips correctly.
func TestWALAppendAndReplay(t *testing.T) {
	path := walPath(t)
	const n = 500

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	appendEntries(t, w, n, false)
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	entries := replayAll(t, path)
	if len(entries) != n {
		t.Fatalf("expected %d entries, got %d", n, len(entries))
	}
	for i, e := range entries {
		wantKey := fmt.Sprintf("key-%06d", i)
		wantVal := fmt.Sprintf("value-%06d", i)
		if string(e.Key) != wantKey {
			t.Errorf("[%d] key = %q, want %q", i, e.Key, wantKey)
		}
		if string(e.Value) != wantVal {
			t.Errorf("[%d] value = %q, want %q", i, e.Value, wantVal)
		}
		if e.Tombstone {
			t.Errorf("[%d] unexpected tombstone", i)
		}
		if e.Timestamp == 0 {
			t.Errorf("[%d] timestamp is zero", i)
		}
	}
}

// TestWALTombstone verifies the tombstone flag survives a roundtrip.
func TestWALTombstone(t *testing.T) {
	path := walPath(t)
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}

	if err := w.Append([]byte("alive"), []byte("val"), false); err != nil {
		t.Fatal(err)
	}
	if err := w.Append([]byte("dead"), nil, true); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	entries := replayAll(t, path)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].Tombstone {
		t.Error("entry 0 should not be a tombstone")
	}
	if !entries[1].Tombstone {
		t.Error("entry 1 should be a tombstone")
	}
	if string(entries[1].Key) != "dead" {
		t.Errorf("entry 1 key = %q, want %q", entries[1].Key, "dead")
	}
	if len(entries[1].Value) != 0 {
		t.Errorf("tombstone value should be empty, got %d bytes", len(entries[1].Value))
	}
}

// TestWALEmptyReplay replays an empty file and expects zero entries.
func TestWALEmptyReplay(t *testing.T) {
	path := walPath(t)

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	entries := replayAll(t, path)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries from empty WAL, got %d", len(entries))
	}
}

// TestWALLargeEntries exercises keys up to 64 KB and values up to 1 MB.
func TestWALLargeEntries(t *testing.T) {
	path := walPath(t)
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	bigKey := bytes.Repeat([]byte("K"), maxKeySize) // 65535 bytes (uint16 max)
	bigVal := bytes.Repeat([]byte("V"), 1024*1024)  // 1 MB

	if err := w.Append(bigKey, bigVal, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	entries := replayAll(t, path)
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Key, bigKey) {
		t.Error("large key mismatch")
	}
	if !bytes.Equal(entries[0].Value, bigVal) {
		t.Error("large value mismatch")
	}
}

// TestWALTornWriteHeader simulates a power failure that left a partial
// (< 19 byte) header at the tail of the log.  Valid entries before the
// torn record must be recovered, and the corrupt tail must be truncated.
func TestWALTornWriteHeader(t *testing.T) {
	path := walPath(t)
	const n = 10

	// Write valid entries.
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	appendEntries(t, w, n, false)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	validSize := fileSize(t, path)

	// Append 10 garbage bytes (simulates a torn header write).
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("XXXXXXXXXX")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if fileSize(t, path) != validSize+10 {
		t.Fatal("garbage bytes not appended")
	}

	// Replay: expect exactly n valid entries.
	entries := replayAll(t, path)
	if len(entries) != n {
		t.Fatalf("got %d entries, want %d", len(entries), n)
	}

	// File should be truncated back to the valid size.
	if sz := fileSize(t, path); sz != validSize {
		t.Errorf("file size after recovery = %d, want %d", sz, validSize)
	}
}

// TestWALTornWritePayload simulates a crash after the header was written
// but before the full payload (key+value) was persisted.
func TestWALTornWritePayload(t *testing.T) {
	path := walPath(t)
	const n = 5

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	appendEntries(t, w, n, false)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	validSize := fileSize(t, path)

	// Append a valid-looking header that claims a 100-byte key and
	// 200-byte value, but only write 50 bytes of payload → torn payload.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [walHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[4:12], 999)  // fake timestamp
	hdr[12] = 0                                      // not tombstone
	binary.LittleEndian.PutUint16(hdr[13:15], 100)   // keySize = 100
	binary.LittleEndian.PutUint32(hdr[15:19], 200)   // valueSize = 200
	// CRC doesn't matter – we won't even get to the CRC check because
	// the payload read will be short.
	binary.LittleEndian.PutUint32(hdr[0:4], 0xDEADBEEF)
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	// Only write 50 bytes of the expected 300 payload bytes.
	if _, err := f.Write(make([]byte, 50)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	entries := replayAll(t, path)
	if len(entries) != n {
		t.Fatalf("got %d entries, want %d", len(entries), n)
	}
	if sz := fileSize(t, path); sz != validSize {
		t.Errorf("file size after recovery = %d, want %d", sz, validSize)
	}
}

// TestWALCorruptCRC writes valid entries, then flips a byte in the last
// record's payload on disk.  The iterator must detect the CRC mismatch,
// truncate the corrupt record, and return only the preceding valid ones.
func TestWALCorruptCRC(t *testing.T) {
	path := walPath(t)
	const n = 8

	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	appendEntries(t, w, n, false)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	totalSize := fileSize(t, path)

	// Each record: 19 header + len("key-XXXXXX") + len("value-XXXXXX")
	//            = 19 + 10 + 12 = 41 bytes
	const recSize = 41
	lastRecordStart := totalSize - recSize

	// Flip a byte in the key portion of the last record.
	// Key starts at lastRecordStart + walHeaderSize.
	corruptOffset := lastRecordStart + walHeaderSize + 2

	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], corruptOffset); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF // flip all bits
	if _, err := f.WriteAt(b[:], corruptOffset); err != nil {
		t.Fatal(err)
	}
	f.Close()

	entries := replayAll(t, path)
	if len(entries) != n-1 {
		t.Fatalf("got %d entries, want %d (last should be dropped)", len(entries), n-1)
	}

	// File should be truncated to exclude the last record.
	if sz := fileSize(t, path); sz != lastRecordStart {
		t.Errorf("file size = %d, want %d", sz, lastRecordStart)
	}
}

// TestWALReopenAppend verifies that closing and reopening a WAL correctly
// appends new records after the existing ones (O_APPEND semantics).
func TestWALReopenAppend(t *testing.T) {
	path := walPath(t)

	// Write first batch.
	w1, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	appendEntries(t, w1, 5, false)
	if err := w1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen and write second batch.
	w2, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 5; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val := []byte(fmt.Sprintf("value-%06d", i))
		if err := w2.Append(key, val, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	// Replay should yield all 10 entries in order.
	entries := replayAll(t, path)
	if len(entries) != 10 {
		t.Fatalf("got %d entries, want 10", len(entries))
	}
	for i, e := range entries {
		want := fmt.Sprintf("key-%06d", i)
		if string(e.Key) != want {
			t.Errorf("[%d] key = %q, want %q", i, e.Key, want)
		}
	}
}

// TestWALSize verifies that Size() accounts for both flushed and
// buffered (unflushed) bytes.
func TestWALSize(t *testing.T) {
	path := walPath(t)
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	s0, err := w.Size()
	if err != nil {
		t.Fatal(err)
	}
	if s0 != 0 {
		t.Fatalf("initial size = %d, want 0", s0)
	}

	// Append a record (stays in bufio buffer until flush).
	if err := w.Append([]byte("k"), []byte("v"), false); err != nil {
		t.Fatal(err)
	}
	s1, err := w.Size()
	if err != nil {
		t.Fatal(err)
	}
	// Expected: 19 (header) + 1 (key) + 1 (value) = 21
	if s1 != 21 {
		t.Fatalf("size after 1 append = %d, want 21", s1)
	}
}

// TestWALKeyTooLarge verifies that Append rejects keys exceeding uint16 max.
func TestWALKeyTooLarge(t *testing.T) {
	path := walPath(t)
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	bigKey := make([]byte, maxKeySize+1)
	if err := w.Append(bigKey, []byte("v"), false); err == nil {
		t.Fatal("expected error for oversized key, got nil")
	}
}

// TestWALTimestampMonotonic verifies that timestamps are monotonically
// non-decreasing across consecutive appends.
func TestWALTimestampMonotonic(t *testing.T) {
	path := walPath(t)
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err := w.Append([]byte("k"), []byte("v"), false); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	entries := replayAll(t, path)
	for i := 1; i < len(entries); i++ {
		if entries[i].Timestamp < entries[i-1].Timestamp {
			t.Errorf("timestamp[%d]=%d < timestamp[%d]=%d — not monotonic",
				i, entries[i].Timestamp, i-1, entries[i-1].Timestamp)
		}
	}
}

// TestWALCRC32CIntegrity manually constructs a record and verifies
// the CRC matches what hash/crc32 computes with Castagnoli.
func TestWALCRC32CIntegrity(t *testing.T) {
	path := walPath(t)
	w, err := OpenWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("hello")
	val := []byte("world")
	if err := w.Append(key, val, false); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read raw bytes and manually verify CRC.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != walHeaderSize+len(key)+len(val) {
		t.Fatalf("file size = %d, want %d", len(raw), walHeaderSize+len(key)+len(val))
	}

	storedCRC := binary.LittleEndian.Uint32(raw[0:4])
	// CRC covers bytes [4 .. end].
	computed := crc32.Checksum(raw[4:], crc32cTable)
	if storedCRC != computed {
		t.Errorf("CRC mismatch: stored=0x%08X computed=0x%08X", storedCRC, computed)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkWALAppend measures the cost of a single Append (no Sync).
// Target: 0 allocs/op on the hot path.
func BenchmarkWALAppend(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.wal")
	w, err := OpenWAL(path)
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	key := []byte("benchmark-key-0123456789")
	val := []byte("benchmark-value-with-some-realistic-payload-data")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.Append(key, val, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkWALAppendBatch measures throughput of batching 1000 appends
// followed by a single Sync, and reports MB/s.
func BenchmarkWALAppendBatch(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_batch.wal")

	key := []byte("batch-key-0123456789")
	val := []byte("batch-value-with-payload-data-for-throughput-measurement")
	recordBytes := int64(walHeaderSize + len(key) + len(val))

	const batchSize = 1000

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(recordBytes * batchSize)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		w, err := OpenWAL(path)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for j := 0; j < batchSize; j++ {
			if err := w.Append(key, val, false); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Sync(); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		w.Close()
		os.Remove(path)
		b.StartTimer()
	}
}

// BenchmarkWALReplay measures the throughput of sequential replay over
// a WAL file containing 100 000 entries.
func BenchmarkWALReplay(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench_replay.wal")

	const numEntries = 100_000
	key := []byte("replay-key-0123456789")
	val := []byte("replay-value-with-some-data")

	// Pre-populate the WAL.
	w, err := OpenWAL(path)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < numEntries; i++ {
		if err := w.Append(key, val, false); err != nil {
			b.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		b.Fatal(err)
	}

	recordBytes := int64(walHeaderSize + len(key) + len(val))
	b.SetBytes(recordBytes * numEntries)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		it, err := NewWALIterator(path)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for {
			e, err := it.Next()
			if err != nil {
				b.Fatal(err)
			}
			if e == nil {
				break
			}
			count++
		}
		it.Close()
		if count != numEntries {
			b.Fatalf("replayed %d, want %d", count, numEntries)
		}
	}
}
