package compaction

import (
	"github.com/shreyas/lsmtree/iterator"
)

// Table defines the read-only interface for an SSTable that can be compacted.
type Table interface {
	Iterator() iterator.Iterator
	BlockCount() int
	Path() string
}

// Plan specifies a set of tables to be merged during compaction.
type Plan struct {
	Tables []Table
}

// purgeIterator wraps an Iterator and drops all tombstones.
// It is only safe to use during a FULL compaction (where no older levels exist).
type purgeIterator struct {
	it iterator.Iterator
}

// Compile-time check to ensure purgeIterator implements iterator.Iterator
var _ iterator.Iterator = (*purgeIterator)(nil)

// NewPurgeIterator creates a new iterator that skips over any tombstones.
func NewPurgeIterator(it iterator.Iterator) iterator.Iterator {
	p := &purgeIterator{it: it}
	p.advanceToValid()
	return p
}

func (p *purgeIterator) advanceToValid() {
	for p.it.Valid() && p.it.Tombstone() {
		p.it.Next()
	}
}

func (p *purgeIterator) Valid() bool       { return p.it.Valid() }
func (p *purgeIterator) Next()             { p.it.Next(); p.advanceToValid() }
func (p *purgeIterator) Key() []byte       { return p.it.Key() }
func (p *purgeIterator) Value() []byte     { return p.it.Value() }
func (p *purgeIterator) Timestamp() uint64 { return p.it.Timestamp() }
func (p *purgeIterator) Tombstone() bool   { return p.it.Tombstone() }
func (p *purgeIterator) Error() error      { return p.it.Error() }
func (p *purgeIterator) Close() error      { return p.it.Close() }
