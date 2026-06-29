package memtable

import (
	"time"

	"github.com/shreyas/lsmtree/iterator"
)

// Compile-time check: Iterator must satisfy iterator.Iterator.
var _ iterator.Iterator = (*Iterator)(nil)

// MemTable is the volatile in-memory staging area for recent writes.
// It wraps a skip list to maintain keys in sorted order at all times,
// enabling efficient sequential flushing to immutable SSTables on disk.
//
// The MemTable tracks its approximate memory consumption so the engine
// can rotate it when a configurable size threshold is reached.
type MemTable struct {
	tree      *redBlackTree
	threshold int64 // size in bytes at which the table is considered full
}

// New creates a MemTable that signals full when its approximate
// memory consumption reaches threshold bytes.
func New(threshold int64) *MemTable {
	return &MemTable{
		tree:      newRedBlackTree(),
		threshold: threshold,
	}
}

// Put inserts or updates a key-value pair with the current wall-clock
// timestamp. To insert with an explicit timestamp (e.g., during WAL
// replay), use PutWithTimestamp.
func (m *MemTable) Put(key, value []byte) {
	m.tree.Put(key, value, uint64(time.Now().UnixNano()), false)
}

// Delete inserts a tombstone marker for key. The tombstone shadows any
// older value in deeper levels (immutable MemTables, SSTables) during
// the read path.
func (m *MemTable) Delete(key []byte) {
	m.tree.Put(key, nil, uint64(time.Now().UnixNano()), true)
}

// PutWithTimestamp inserts an entry with an explicit timestamp and
// tombstone flag. This is used during WAL replay to reconstruct the
func (m *MemTable) PutWithTimestamp(key, value []byte, timestamp uint64, tombstone bool) {
	m.tree.Put(key, value, timestamp, tombstone)
}

// Get looks up key and returns the stored value and deletion status.
//
// Return values:
//   - (value, true, false)  — key exists, live entry
//   - (nil,   true, true)   — key exists, tombstone (deleted)
//   - (nil,   false, false) — key not found
func (m *MemTable) Get(key []byte) (value []byte, found bool, isDeleted bool) {
	val, _, found, tomb := m.tree.Get(key)
	return val, found, tomb
}

// IsFull returns true when the approximate memory usage has reached
// or exceeded the configured threshold.
func (m *MemTable) IsFull() bool {
	return m.tree.Size() >= m.threshold
}

func (m *MemTable) Size() int64 {
	return m.tree.Size()
}

func (m *MemTable) Len() int {
	return m.tree.Len()
}

// Iterator — ordered traversal for SSTable flushing

// Iterator walks the MemTable entries in sorted key order.
// It is designed to be used on immutable (rotated) MemTables during
// flushing, where no concurrent writes occur.
type Iterator struct {
	node *rbNode
}

// NewIterator returns a forward iterator positioned at the smallest key.
// The iterator walks the level-0 linked list, which contains every
// entry in strict lexicographic order.
//
// The iterator is only safe to use when no concurrent writes are
// happening (i.e., on an immutable MemTable that has been rotated).
func (m *MemTable) NewIterator() *Iterator {
	return &Iterator{node: m.tree.front()}
}

func (it *Iterator) Valid() bool {
	return it.node != nil
}

func (it *Iterator) Next() {
	it.node = successor(it.node)
}

func (it *Iterator) Key() []byte {
	return it.node.key
}

func (it *Iterator) Value() []byte {
	return it.node.value
}

func (it *Iterator) Timestamp() uint64 {
	return it.node.timestamp
}

func (it *Iterator) Tombstone() bool {
	return it.node.tombstone
}

// Error returns any error encountered during iteration. MemTable iteration
// is entirely in-memory and never fails, so this always returns nil.
func (it *Iterator) Error() error {
	return nil
}

func (it *Iterator) Close() error {
	return nil
}
