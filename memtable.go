package lsmtree

import "time"

// MemTable is the volatile in-memory staging area for recent writes.
// It wraps a skip list to maintain keys in sorted order at all times,
// enabling efficient sequential flushing to immutable SSTables on disk.
//
// The MemTable tracks its approximate memory consumption so the engine
// can rotate it when a configurable size threshold is reached.
type MemTable struct {
	list      *skipList
	threshold int64 // size in bytes at which the table is considered full
}

// NewMemTable creates a MemTable that signals full when its approximate
// memory consumption reaches threshold bytes.
func NewMemTable(threshold int64) *MemTable {
	return &MemTable{
		list:      newSkipList(),
		threshold: threshold,
	}
}

// Put inserts or updates a key-value pair with the current wall-clock
// timestamp. To insert with an explicit timestamp (e.g., during WAL
// replay), use PutWithTimestamp.
func (m *MemTable) Put(key, value []byte) {
	m.list.Put(key, value, uint64(time.Now().UnixNano()), false)
}

// Delete inserts a tombstone marker for key. The tombstone shadows any
// older value in deeper levels (immutable MemTables, SSTables) during
// the read path.
func (m *MemTable) Delete(key []byte) {
	m.list.Put(key, nil, uint64(time.Now().UnixNano()), true)
}

// PutWithTimestamp inserts an entry with an explicit timestamp and
// tombstone flag. This is used during WAL replay to reconstruct the
// MemTable with the original timestamps from the log.
func (m *MemTable) PutWithTimestamp(key, value []byte, timestamp uint64, tombstone bool) {
	m.list.Put(key, value, timestamp, tombstone)
}

// Get looks up key and returns the stored value and deletion status.
//
// Return values:
//   - (value, true, false)  — key exists, live entry
//   - (nil,   true, true)   — key exists, tombstone (deleted)
//   - (nil,   false, false) — key not found
//
// The returned value slice references internal memory; do not modify it.
// This design keeps the read path at zero heap allocations.
func (m *MemTable) Get(key []byte) (value []byte, found bool, isDeleted bool) {
	val, _, found, tomb := m.list.Get(key)
	return val, found, tomb
}

// IsFull returns true when the approximate memory usage has reached
// or exceeded the configured threshold.
func (m *MemTable) IsFull() bool {
	return m.list.Size() >= m.threshold
}

// Size returns the approximate memory usage in bytes.
func (m *MemTable) Size() int64 {
	return m.list.Size()
}

// Len returns the number of entries (including tombstones).
func (m *MemTable) Len() int {
	return m.list.Len()
}

// ---------------------------------------------------------------------------
// MemTableIterator — ordered traversal for SSTable flushing
// ---------------------------------------------------------------------------

// MemTableIterator walks the MemTable entries in sorted key order.
// It is designed to be used on immutable (rotated) MemTables during
// flushing, where no concurrent writes occur.
type MemTableIterator struct {
	node *skipListNode
}

// Iterator returns a forward iterator positioned at the smallest key.
// The iterator walks the level-0 linked list, which contains every
// entry in strict lexicographic order.
//
// The iterator is only safe to use when no concurrent writes are
// happening (i.e., on an immutable MemTable that has been rotated).
func (m *MemTable) Iterator() *MemTableIterator {
	return &MemTableIterator{node: m.list.front()}
}

// Valid reports whether the iterator is positioned at a valid entry.
func (it *MemTableIterator) Valid() bool {
	return it.node != nil
}

// Next advances the iterator to the next entry in sort order.
func (it *MemTableIterator) Next() {
	if it.node != nil {
		it.node = it.node.next[0]
	}
}

// Key returns the current entry's key. Do not modify the returned slice.
func (it *MemTableIterator) Key() []byte {
	return it.node.key
}

// Value returns the current entry's value. Do not modify the returned slice.
func (it *MemTableIterator) Value() []byte {
	return it.node.value
}

// Timestamp returns the current entry's timestamp.
func (it *MemTableIterator) Timestamp() uint64 {
	return it.node.timestamp
}

// Tombstone reports whether the current entry is a deletion marker.
func (it *MemTableIterator) Tombstone() bool {
	return it.node.tombstone
}
