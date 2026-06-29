package iterator

// Iterator defines the contract for sequential, ordered traversal over
// key-value entries.
//
// Usage pattern:
//
//	it := source.Iterator()
//	defer it.Close()
//	for it.Valid() {
//	    key := it.Key()
//	    val := it.Value()
//	    // process entry...
//	    it.Next()
//	}
//	if err := it.Error(); err != nil {
//	    // handle error
//	}
type Iterator interface {
	// Valid returns true if the iterator is positioned at a valid entry.
	// When Valid returns false, the caller should check Error() to
	// distinguish between normal exhaustion (nil) and a failure.
	Valid() bool

	// Next advances the iterator to the next entry in sort order.
	// Must only be called when Valid() is true.
	Next()

	// Key returns the current entry's key. The returned slice is only
	// valid until the next call to Next() and must not be modified by
	// the caller.
	Key() []byte

	// Value returns the current entry's value. The returned slice is
	// only valid until the next call to Next() and must not be modified
	// by the caller. Returns nil for tombstone entries.
	Value() []byte

	// Timestamp returns the current entry's monotonic sequence number.
	// Higher timestamps indicate more recent writes.
	Timestamp() uint64

	// Tombstone reports whether the current entry is a deletion marker.
	Tombstone() bool

	// Error returns any error encountered during iteration. Returns nil
	// if iteration completed successfully or hasn't encountered an error.
	Error() error

	// Close releases any resources held by the iterator (e.g., file
	// handles). Must be called when the iterator is no longer needed.
	Close() error
}
