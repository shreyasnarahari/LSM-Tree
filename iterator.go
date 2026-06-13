package lsmtree

// Iterator defines the standard interface for sequential, ordered traversal
// over any data source in the LSM Tree (e.g., MemTable, SSTable, MergeIterator).
//
// Iterators must yield keys in strictly ascending lexicographic order.
// If an iterator encounters an unrecoverable error during iteration,
// Valid() should return false, and Error() should return the underlying error.
type Iterator interface {
	// Valid returns true if the iterator is positioned at a valid entry.
	Valid() bool

	// Next advances the iterator to the next entry.
	Next()

	// Key returns the current entry's key. The returned slice is only
	// valid until the next call to Next() and must not be modified.
	Key() []byte

	// Value returns the current entry's value. The returned slice is only
	// valid until the next call to Next() and must not be modified.
	Value() []byte

	// Timestamp returns the current entry's timestamp.
	Timestamp() uint64

	// Tombstone reports whether the current entry is a deletion marker.
	Tombstone() bool

	// Error returns any error encountered during iteration.
	Error() error

	// Close releases any resources held by the iterator.
	Close() error
}
