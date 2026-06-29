package compaction

import (
	"bytes"
	"container/heap"

	"github.com/shreyas/lsmtree/internal"
	"github.com/shreyas/lsmtree/iterator"
)

// MergeIterator merges multiple Iterators into a single ordered stream.
// It resolves duplicate keys by favouring iterators that appear earlier
// in the input list (i.e., index 0 is newest/highest priority).
type MergeIterator struct {
	h mergeHeap

	key       []byte
	value     []byte
	timestamp uint64
	tombstone bool
	valid     bool
	err       error
}

// Compile-time check to ensure MergeIterator implements iterator.Iterator
var _ iterator.Iterator = (*MergeIterator)(nil)

// NewMergeIterator creates a MergeIterator from a list of Iterators.
func NewMergeIterator(iters []iterator.Iterator) *MergeIterator {
	m := &MergeIterator{
		h: make(mergeHeap, 0, len(iters)),
	}
	for i, it := range iters {
		if it != nil && it.Valid() {
			m.h = append(m.h, &heapNode{it: it, idx: i})
		} else if it != nil && it.Error() != nil {
			m.err = it.Error()
			return m
		}
	}
	heap.Init(&m.h)
	m.Next() // Load first entry
	return m
}

func (m *MergeIterator) Valid() bool {
	return m.valid && m.err == nil
}

func (m *MergeIterator) Next() {
	if m.err != nil {
		m.valid = false
		return
	}

	if m.h.Len() == 0 {
		m.valid = false
		m.key = nil
		return
	}

	// 1. Pop the smallest key (highest priority if keys are equal).
	node := heap.Pop(&m.h).(*heapNode)

	// Clone key/value because the underlying iterators reuse their buffers or
	// memory-mapped slices that might change on Next().
	m.key = internal.CloneBytes(node.it.Key())
	m.value = internal.CloneBytes(node.it.Value())
	m.timestamp = node.it.Timestamp()
	m.tombstone = node.it.Tombstone()
	m.valid = true

	// 2. Advance the winning iterator.
	node.it.Next()
	if err := node.it.Error(); err != nil {
		m.err = err
		return
	}
	if node.it.Valid() {
		heap.Push(&m.h, node)
	}

	// 3. Discard older versions of the same key from other iterators.
	for m.h.Len() > 0 && bytes.Equal(m.h[0].it.Key(), m.key) {
		dup := heap.Pop(&m.h).(*heapNode)
		dup.it.Next()
		if err := dup.it.Error(); err != nil {
			m.err = err
			return
		}
		if dup.it.Valid() {
			heap.Push(&m.h, dup)
		}
	}
}

func (m *MergeIterator) Key() []byte {
	return m.key
}

func (m *MergeIterator) Value() []byte {
	return m.value
}

func (m *MergeIterator) Timestamp() uint64 {
	return m.timestamp
}

func (m *MergeIterator) Tombstone() bool {
	return m.tombstone
}

func (m *MergeIterator) Error() error {
	return m.err
}

func (m *MergeIterator) Close() error {
	var firstErr error
	for _, node := range m.h {
		if err := node.it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// min-heap implementation

type heapNode struct {
	it  iterator.Iterator
	idx int // priority: smaller is newer
}

type mergeHeap []*heapNode

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].it.Key(), h[j].it.Key())
	if cmp == 0 {
		return h[i].idx < h[j].idx
	}
	return cmp < 0
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*heapNode))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return item
}
