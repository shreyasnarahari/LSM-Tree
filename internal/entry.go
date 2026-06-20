// Package internal defines the foundational data types used across every
// component of the LSM Tree engine: MemTable, WAL, SSTable, and Compaction.
//
// By centralising the canonical entry representation here, we avoid
// duplicating struct definitions and ensure consistent semantics
// (especially around tombstone handling and timestamp ordering)
// throughout the codebase.
package internal

import "bytes"

// OpType distinguishes live entries from deletion markers (tombstones).
//
// Every mutation in the LSM tree carries an OpType so that the read
// path can distinguish "key exists with this value" from "key was
// explicitly deleted." Tombstones must propagate through immutable
// MemTables and SSTables until compaction at the deepest level can
// safely discard them.
type OpType byte

const (
	// OpPut indicates a live key-value entry.
	OpPut OpType = 0x01

	// OpDelete indicates a tombstone — the key has been logically deleted.
	// The Value field of a deleted entry is always nil.
	OpDelete OpType = 0x02
)

// Entry is the fundamental unit of data flowing through the LSM tree.
//
// It represents a single key-value mutation (put or delete) at a
// specific point in time. Entries are written to the WAL, stored in
// the MemTable, serialised into SSTable data blocks, and compared
// during compaction merges.
//
// Fields:
//   - Key:       The lookup key. Must not be nil or empty.
//   - Value:     The stored value. Nil for tombstone entries (Op == OpDelete).
//   - Timestamp: A monotonically increasing sequence number used to
//     resolve conflicts when the same key appears in multiple
//     data sources (e.g., MemTable vs. SSTable).
//   - Op:        Whether this entry is a put or a delete.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Op        OpType
}

// IsDeleted reports whether this entry is a tombstone (deletion marker).
func (e *Entry) IsDeleted() bool {
	return e.Op == OpDelete
}

// CompareKeys performs a lexicographic comparison of two keys.
//
// Returns:
//
//	-1 if a < b
//	 0 if a == b
//	+1 if a > b
func CompareKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

// CloneBytes returns a copy of b, or nil if b is nil.
//
// This is used throughout the engine to ensure that byte slices
// stored internally are never aliased with caller-owned memory.
func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
