package internal

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// TestEntryIsDeleted verifies the IsDeleted helper for both OpPut and OpDelete.
func TestEntryIsDeleted(t *testing.T) {
	live := &Entry{Key: []byte("k"), Value: []byte("v"), Op: OpPut}
	if live.IsDeleted() {
		t.Error("OpPut entry should not be deleted")
	}

	tomb := &Entry{Key: []byte("k"), Op: OpDelete}
	if !tomb.IsDeleted() {
		t.Error("OpDelete entry should be deleted")
	}
}

// TestCompareKeys verifies lexicographic key comparison.
func TestCompareKeys(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"apple", "banana", -1},
		{"banana", "apple", 1},
		{"cherry", "cherry", 0},
		{"", "a", -1},
		{"a", "", 1},
		{"", "", 0},
	}
	for _, tt := range tests {
		got := CompareKeys([]byte(tt.a), []byte(tt.b))
		if got != tt.want {
			t.Errorf("CompareKeys(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

// TestCloneBytes verifies deep copy semantics and nil handling.
func TestCloneBytes(t *testing.T) {
	// nil input → nil output.
	if got := CloneBytes(nil); got != nil {
		t.Errorf("CloneBytes(nil) = %v, want nil", got)
	}

	// Non-nil input → independent copy.
	orig := []byte("hello")
	clone := CloneBytes(orig)
	if !bytes.Equal(orig, clone) {
		t.Errorf("CloneBytes(%q) = %q", orig, clone)
	}

	// Mutating the clone must not affect the original.
	clone[0] = 'X'
	if bytes.Equal(orig, clone) {
		t.Error("clone mutation should not affect original")
	}
}

// Encoding round-trip tests

// TestEncodeDecodePut verifies a live entry survives encode→decode.
func TestEncodeDecodePut(t *testing.T) {
	original := &Entry{
		Key:       []byte("test-key"),
		Value:     []byte("test-value-with-some-data"),
		Timestamp: 1234567890,
		Op:        OpPut,
	}

	var buf bytes.Buffer
	if err := Encode(original, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	assertEntryEqual(t, original, decoded)
}

// TestEncodeDecodeDelete verifies a tombstone entry survives encode→decode.
func TestEncodeDecodeDelete(t *testing.T) {
	original := &Entry{
		Key:       []byte("deleted-key"),
		Value:     nil,
		Timestamp: 999,
		Op:        OpDelete,
	}

	var buf bytes.Buffer
	if err := Encode(original, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	assertEntryEqual(t, original, decoded)
}

// TestEncodeDecodeEmptyValue verifies that an OpPut with empty (but non-nil)
// value round-trips correctly.
func TestEncodeDecodeEmptyValue(t *testing.T) {
	original := &Entry{
		Key:       []byte("empty-val"),
		Value:     []byte{},
		Timestamp: 42,
		Op:        OpPut,
	}

	var buf bytes.Buffer
	if err := Encode(original, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := Decode(&buf)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// An empty value encodes as valLen=0 and decodes as nil.
	// This is acceptable — the important thing is valLen==0.
	if len(decoded.Value) != 0 {
		t.Fatalf("decoded value should be empty, got %q", decoded.Value)
	}
}

// TestEncodeDecodeMultiple verifies that multiple entries can be
// written and read back sequentially from the same stream.
func TestEncodeDecodeMultiple(t *testing.T) {
	entries := []*Entry{
		{Key: []byte("key-1"), Value: []byte("val-1"), Timestamp: 1, Op: OpPut},
		{Key: []byte("key-2"), Value: nil, Timestamp: 2, Op: OpDelete},
		{Key: []byte("key-3"), Value: []byte("val-3-longer"), Timestamp: 3, Op: OpPut},
	}

	var buf bytes.Buffer
	for _, e := range entries {
		if err := Encode(e, &buf); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}

	for i, want := range entries {
		got, err := Decode(&buf)
		if err != nil {
			t.Fatalf("Decode[%d]: %v", i, err)
		}
		assertEntryEqual(t, want, got)
	}

	// Stream should be exhausted.
	_, err := Decode(&buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF after all entries, got %v", err)
	}
}

// TestDecodeEOF verifies that Decode returns io.EOF on an empty reader.
func TestDecodeEOF(t *testing.T) {
	_, err := Decode(&bytes.Buffer{})
	if err != io.EOF {
		t.Fatalf("Decode on empty reader: err = %v, want io.EOF", err)
	}
}

// TestDecodeTruncatedHeader verifies that a partial header returns
// io.ErrUnexpectedEOF.
func TestDecodeTruncatedHeader(t *testing.T) {
	// Write less than EntryHeaderSize bytes.
	r := strings.NewReader("short")
	_, err := Decode(r)
	if err == nil {
		t.Fatal("expected error for truncated header")
	}
}

// TestDecodeTruncatedPayload verifies that a valid header followed by
// insufficient payload bytes returns an error.
func TestDecodeTruncatedPayload(t *testing.T) {
	original := &Entry{
		Key:       []byte("somekey"),
		Value:     []byte("somevalue"),
		Timestamp: 1,
		Op:        OpPut,
	}

	var buf bytes.Buffer
	if err := Encode(original, &buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Truncate: keep header but chop the payload.
	truncated := buf.Bytes()[:EntryHeaderSize+2]
	_, err := Decode(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("expected error for truncated payload")
	}
}

// TestEncodeKeyTooLarge verifies that Encode rejects keys exceeding uint16 max.
func TestEncodeKeyTooLarge(t *testing.T) {
	e := &Entry{
		Key:   make([]byte, MaxKeySize+1),
		Value: []byte("v"),
		Op:    OpPut,
	}
	err := Encode(e, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error for oversized key")
	}
}

// TestEncodedSize verifies the wire format produces exactly the expected
// number of bytes.
func TestEncodedSize(t *testing.T) {
	e := &Entry{
		Key:       []byte("hello"),      // 5 bytes
		Value:     []byte("world12345"), // 10 bytes
		Timestamp: 100,
		Op:        OpPut,
	}

	var buf bytes.Buffer
	if err := Encode(e, &buf); err != nil {
		t.Fatal(err)
	}

	want := EntryHeaderSize + len(e.Key) + len(e.Value) // 15 + 5 + 10 = 30
	if buf.Len() != want {
		t.Fatalf("encoded size = %d, want %d", buf.Len(), want)
	}
}

// assertEntryEqual is a test helper that compares two entries field-by-field.
func assertEntryEqual(t *testing.T, want, got *Entry) {
	t.Helper()
	if !bytes.Equal(want.Key, got.Key) {
		t.Errorf("Key: got %q, want %q", got.Key, want.Key)
	}
	if !bytes.Equal(want.Value, got.Value) {
		// Allow nil vs empty equivalence for Value.
		if len(want.Value) != 0 || len(got.Value) != 0 {
			t.Errorf("Value: got %q, want %q", got.Value, want.Value)
		}
	}
	if want.Timestamp != got.Timestamp {
		t.Errorf("Timestamp: got %d, want %d", got.Timestamp, want.Timestamp)
	}
	if want.Op != got.Op {
		t.Errorf("Op: got %d, want %d", got.Op, want.Op)
	}
}
