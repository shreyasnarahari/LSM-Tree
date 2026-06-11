package lsmtree

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxHeight   = 32
	probability = 0.25

	// Memory overhead estimates for size tracking (64-bit arch).
	// skipListNode struct: key(24) + value(24) + timestamp(8) + tombstone(1) + pad(7) + next(24) = 88
	nodeBaseSize int64 = 88
	ptrSize      int64 = 8
)

// skipListNode is a single element in the skip list.
type skipListNode struct {
	key       []byte
	value     []byte
	timestamp uint64
	tombstone bool
	next      []*skipListNode // forward pointers, one per level
}

// skipList is a probabilistically balanced sorted data structure providing
// O(log N) insert and lookup. Keys are maintained in lexicographic order
// at all times, enabling efficient sequential iteration for SSTable flushing.
//
// Concurrency: sync.RWMutex guards all access. Multiple concurrent readers
// are allowed (RLock); writes are exclusive (Lock).
type skipList struct {
	head   *skipListNode
	height int // current max level in use (1-based)
	length int
	mu     sync.RWMutex
	rng    *rand.Rand
	size   atomic.Int64 // approximate memory usage in bytes
}

// newSkipList creates an empty skip list.
func newSkipList() *skipList {
	return &skipList{
		head:   &skipListNode{next: make([]*skipListNode, maxHeight)},
		height: 1,
		rng:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// randomHeight returns a random level for a new node using geometric
// distribution with p=0.25. Expected height ≈ 1.33 levels.
func (sl *skipList) randomHeight() int {
	h := 1
	for h < maxHeight && sl.rng.Float64() < probability {
		h++
	}
	return h
}

// nodeMemSize returns the approximate heap bytes consumed by a node.
func nodeMemSize(height, keyLen, valLen int) int64 {
	return nodeBaseSize + int64(height)*ptrSize + int64(keyLen) + int64(valLen)
}

// Put inserts or updates a key in the skip list.
// Key and value bytes are copied to avoid aliasing with the caller.
func (sl *skipList) Put(key, value []byte, timestamp uint64, tombstone bool) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// update[i] will hold the predecessor node at level i.
	var update [maxHeight]*skipListNode // stack-allocated, 256 bytes
	cur := sl.head
	for i := sl.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
		update[i] = cur
	}

	// Check if key already exists at level 0.
	target := cur.next[0]
	if target != nil && bytes.Equal(target.key, key) {
		// Update in place: adjust size for value change.
		oldValLen := int64(len(target.value))
		target.value = cloneBytes(value)
		target.timestamp = timestamp
		target.tombstone = tombstone
		sl.size.Add(int64(len(value)) - oldValLen)
		return
	}

	// Insert new node.
	h := sl.randomHeight()
	node := &skipListNode{
		key:       cloneBytes(key),
		value:     cloneBytes(value),
		timestamp: timestamp,
		tombstone: tombstone,
		next:      make([]*skipListNode, h),
	}

	if h > sl.height {
		for i := sl.height; i < h; i++ {
			update[i] = sl.head
		}
		sl.height = h
	}

	for i := 0; i < h; i++ {
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}

	sl.length++
	sl.size.Add(nodeMemSize(h, len(key), len(value)))
}

// Get searches for key and returns the stored value and metadata.
// Returns directly into the internal slice (no copy) for zero-alloc reads.
func (sl *skipList) Get(key []byte) (value []byte, timestamp uint64, found, tombstone bool) {
	sl.mu.RLock()
	defer sl.mu.RUnlock()

	cur := sl.head
	for i := sl.height - 1; i >= 0; i-- {
		for cur.next[i] != nil && bytes.Compare(cur.next[i].key, key) < 0 {
			cur = cur.next[i]
		}
	}

	target := cur.next[0]
	if target != nil && bytes.Equal(target.key, key) {
		return target.value, target.timestamp, true, target.tombstone
	}
	return nil, 0, false, false
}

// Len returns the number of entries.
func (sl *skipList) Len() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.length
}

// Size returns the approximate memory usage in bytes.
func (sl *skipList) Size() int64 {
	return sl.size.Load()
}

// front returns the first real node (level 0), or nil if empty.
// Caller must hold at least an RLock.
func (sl *skipList) front() *skipListNode {
	return sl.head.next[0]
}

// cloneBytes returns a copy of b, or nil if b is nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
