package lsmtree

import (
	"testing"
)

// mockIterator implements Iterator for testing MergeIterator.
type mockIterator struct {
	entries []WALEntry
	idx     int
}

func (m *mockIterator) Valid() bool { return m.idx < len(m.entries) }
func (m *mockIterator) Next()       { m.idx++ }
func (m *mockIterator) Key() []byte { return m.entries[m.idx].Key }
func (m *mockIterator) Value() []byte { return m.entries[m.idx].Value }
func (m *mockIterator) Timestamp() uint64 { return m.entries[m.idx].Timestamp }
func (m *mockIterator) Tombstone() bool { return m.entries[m.idx].Tombstone }
func (m *mockIterator) Error() error { return nil }
func (m *mockIterator) Close() error { return nil }

func TestMergeIteratorSimple(t *testing.T) {
	it1 := &mockIterator{
		entries: []WALEntry{
			{Key: []byte("a"), Value: []byte("val-a1"), Timestamp: 2},
			{Key: []byte("c"), Value: []byte("val-c1"), Timestamp: 2},
		},
	}
	it2 := &mockIterator{
		entries: []WALEntry{
			{Key: []byte("b"), Value: []byte("val-b2"), Timestamp: 1},
			{Key: []byte("d"), Value: []byte("val-d2"), Timestamp: 1},
		},
	}

	merge := NewMergeIterator([]Iterator{it1, it2})

	wantKeys := []string{"a", "b", "c", "d"}
	wantVals := []string{"val-a1", "val-b2", "val-c1", "val-d2"}

	idx := 0
	for merge.Valid() {
		if string(merge.Key()) != wantKeys[idx] {
			t.Fatalf("step %d: got key %q, want %q", idx, merge.Key(), wantKeys[idx])
		}
		if string(merge.Value()) != wantVals[idx] {
			t.Fatalf("step %d: got val %q, want %q", idx, merge.Value(), wantVals[idx])
		}
		merge.Next()
		idx++
	}

	if idx != len(wantKeys) {
		t.Fatalf("merged length = %d, want %d", idx, len(wantKeys))
	}
}

func TestMergeIteratorShadowing(t *testing.T) {
	// it1 is newer than it2
	it1 := &mockIterator{
		entries: []WALEntry{
			{Key: []byte("a"), Value: []byte("val-a-new"), Timestamp: 3},
			{Key: []byte("b"), Tombstone: true, Timestamp: 3},
		},
	}
	it2 := &mockIterator{
		entries: []WALEntry{
			{Key: []byte("a"), Value: []byte("val-a-old"), Timestamp: 1},
			{Key: []byte("b"), Value: []byte("val-b-old"), Timestamp: 1},
			{Key: []byte("c"), Value: []byte("val-c-old"), Timestamp: 1},
		},
	}

	merge := NewMergeIterator([]Iterator{it1, it2})

	// "a" should be "val-a-new", "b" should be tombstone, "c" should be "val-c-old"
	type result struct {
		k, v string
		tomb bool
	}
	wants := []result{
		{"a", "val-a-new", false},
		{"b", "", true},
		{"c", "val-c-old", false},
	}

	idx := 0
	for merge.Valid() {
		gotK, gotV, gotTomb := string(merge.Key()), string(merge.Value()), merge.Tombstone()
		want := wants[idx]
		
		if gotK != want.k || gotV != want.v || gotTomb != want.tomb {
			t.Fatalf("step %d: got (%q, %q, %v), want (%q, %q, %v)",
				idx, gotK, gotV, gotTomb, want.k, want.v, want.tomb)
		}
		merge.Next()
		idx++
	}

	if idx != len(wants) {
		t.Fatalf("merged length = %d, want %d", idx, len(wants))
	}
}

func TestMergeIteratorMultiShadow(t *testing.T) {
	it1 := &mockIterator{entries: []WALEntry{{Key: []byte("a"), Value: []byte("1")}}}
	it2 := &mockIterator{entries: []WALEntry{{Key: []byte("a"), Value: []byte("2")}}}
	it3 := &mockIterator{entries: []WALEntry{{Key: []byte("a"), Value: []byte("3")}}}

	merge := NewMergeIterator([]Iterator{it1, it2, it3})
	
	if !merge.Valid() || string(merge.Key()) != "a" || string(merge.Value()) != "1" {
		t.Fatalf("got %q, want '1'", merge.Value())
	}
	merge.Next()
	if merge.Valid() {
		t.Fatalf("expected EOF after first unique key")
	}
}
