package memtable

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/shreyas/lsmtree/internal"
)

type color bool

const (
	red   color = true
	black color = false

	// rbNode struct: key(24) + value(24) + timestamp(8) + tombstone(1) + color(1) + pad(6) + left(8) + right(8) + parent(8) = 88
	nodeBaseSize int64 = 88
)

type rbNode struct {
	key       []byte
	value     []byte
	timestamp uint64
	tombstone bool
	color     color
	left      *rbNode
	right     *rbNode
	parent    *rbNode
}

// redBlackTree is a deterministic self-balancing binary search tree.
// Concurrency: sync.RWMutex guards all access.
type redBlackTree struct {
	root   *rbNode
	length int
	size   atomic.Int64
	mu     sync.RWMutex
}

func newRedBlackTree() *redBlackTree {
	return &redBlackTree{}
}

func nodeMemSize(keyLen, valLen int) int64 {
	return nodeBaseSize + int64(keyLen) + int64(valLen)
}

// Put inserts or updates a key in the tree.
func (t *redBlackTree) Put(key, value []byte, timestamp uint64, tombstone bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	var parent *rbNode
	cur := t.root
	var cmp int

	// Find insertion point
	for cur != nil {
		parent = cur
		cmp = bytes.Compare(key, cur.key)
		if cmp < 0 {
			cur = cur.left
		} else if cmp > 0 {
			cur = cur.right
		} else {
			// Update in place
			oldValLen := int64(len(cur.value))
			cur.value = internal.CloneBytes(value)
			cur.timestamp = timestamp
			cur.tombstone = tombstone
			t.size.Add(int64(len(value)) - oldValLen)
			return
		}
	}

	// Insert new node
	node := &rbNode{
		key:       internal.CloneBytes(key),
		value:     internal.CloneBytes(value),
		timestamp: timestamp,
		tombstone: tombstone,
		color:     red,
		parent:    parent,
	}

	if parent == nil {
		t.root = node
	} else if cmp < 0 {
		parent.left = node
	} else {
		parent.right = node
	}

	t.fixup(node)

	t.length++
	t.size.Add(nodeMemSize(len(key), len(value)))
}

func (t *redBlackTree) fixup(node *rbNode) {
	for node != t.root && node.parent.color == red {
		if node.parent == node.parent.parent.left {
			y := node.parent.parent.right // uncle
			if y != nil && y.color == red {
				node.parent.color = black
				y.color = black
				node.parent.parent.color = red
				node = node.parent.parent
			} else {
				if node == node.parent.right {
					node = node.parent
					t.leftRotate(node)
				}
				node.parent.color = black
				node.parent.parent.color = red
				t.rightRotate(node.parent.parent)
			}
		} else {
			y := node.parent.parent.left // uncle
			if y != nil && y.color == red {
				node.parent.color = black
				y.color = black
				node.parent.parent.color = red
				node = node.parent.parent
			} else {
				if node == node.parent.left {
					node = node.parent
					t.rightRotate(node)
				}
				node.parent.color = black
				node.parent.parent.color = red
				t.leftRotate(node.parent.parent)
			}
		}
	}
	t.root.color = black
}

func (t *redBlackTree) leftRotate(x *rbNode) {
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		t.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}

func (t *redBlackTree) rightRotate(y *rbNode) {
	x := y.left
	y.left = x.right
	if x.right != nil {
		x.right.parent = y
	}
	x.parent = y.parent
	if y.parent == nil {
		t.root = x
	} else if y == y.parent.right {
		y.parent.right = x
	} else {
		y.parent.left = x
	}
	x.right = y
	y.parent = x
}

// Get searches for key and returns the stored value and metadata.
func (t *redBlackTree) Get(key []byte) (value []byte, timestamp uint64, found, tombstone bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	cur := t.root
	for cur != nil {
		cmp := bytes.Compare(key, cur.key)
		if cmp < 0 {
			cur = cur.left
		} else if cmp > 0 {
			cur = cur.right
		} else {
			return cur.value, cur.timestamp, true, cur.tombstone
		}
	}
	return nil, 0, false, false
}

func (t *redBlackTree) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.length
}

func (t *redBlackTree) Size() int64 {
	return t.size.Load()
}

// min returns the node with the minimum key in the subtree rooted at x.
func min(x *rbNode) *rbNode {
	if x == nil {
		return nil
	}
	for x.left != nil {
		x = x.left
	}
	return x
}

// front returns the node with the absolute minimum key in the tree.
// Caller must hold at least an RLock.
func (t *redBlackTree) front() *rbNode {
	return min(t.root)
}

// successor returns the node with the smallest key greater than x.key.
func successor(x *rbNode) *rbNode {
	if x == nil {
		return nil
	}
	if x.right != nil {
		return min(x.right)
	}
	p := x.parent
	for p != nil && x == p.right {
		x = p
		p = p.parent
	}
	return p
}
