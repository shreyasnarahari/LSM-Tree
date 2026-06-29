package compaction

import (
	"github.com/shreyas/lsmtree/iterator"
)

type Table interface {
	Iterator() iterator.Iterator
	BlockCount() int
	Path() string
}

type Plan struct {
	Tables []Table
}

// purgeIterator wraps an Iterator and drops all tombstones.
type purgeIterator struct {
	it iterator.Iterator
}

// Compile-time check to ensure purgeIterator implements iterator.Iterator
var _ iterator.Iterator = (*purgeIterator)(nil)

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
