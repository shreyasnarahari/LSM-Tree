package lsmtree

import (
	"encoding/binary"
	"fmt"
	"io"
)

// SSTableIterator sequentially traverses all entries in an SSTable.
type SSTableIterator struct {
	reader      *SSTableReader
	blockIdx    int
	blockData   []byte
	blockOff    int
	numEntries  uint32
	entriesRead uint32

	// current entry state
	key       []byte
	value     []byte
	timestamp uint64
	tombstone bool

	err error
}

// NewIterator creates a new SSTableIterator.
func (r *SSTableReader) Iterator() *SSTableIterator {
	it := &SSTableIterator{
		reader: r,
	}
	it.loadNextBlock()
	it.Next() // Load the first entry
	return it
}

func (it *SSTableIterator) loadNextBlock() {
	if it.err != nil {
		return
	}
	if it.blockIdx >= len(it.reader.index) {
		it.blockData = nil // No more blocks
		return
	}

	entry := it.reader.index[it.blockIdx]
	it.blockIdx++

	block := make([]byte, entry.length)
	if _, err := it.reader.file.ReadAt(block, int64(entry.offset)); err != nil && err != io.EOF {
		it.err = fmt.Errorf("sstable_iterator: read block: %w", err)
		it.blockData = nil
		return
	}

	if len(block) < blockHeaderSize {
		it.err = fmt.Errorf("sstable_iterator: block too small")
		it.blockData = nil
		return
	}

	it.numEntries = binary.LittleEndian.Uint32(block[0:blockHeaderSize])
	it.entriesRead = 0
	it.blockOff = blockHeaderSize
	it.blockData = block
}

// Valid returns true if the iterator is positioned at a valid entry.
func (it *SSTableIterator) Valid() bool {
	return it.err == nil && it.key != nil
}

// Next advances the iterator to the next entry.
func (it *SSTableIterator) Next() {
	if it.err != nil {
		it.key = nil
		return
	}

	if it.blockData == nil {
		it.key = nil
		return
	}

	if it.entriesRead >= it.numEntries {
		it.loadNextBlock()
		if it.blockData == nil {
			it.key = nil
			return
		}
	}

	block := it.blockData
	off := it.blockOff

	if off+entryHeaderSize > len(block) {
		it.err = fmt.Errorf("sstable_iterator: truncated entry header")
		it.key = nil
		return
	}

	keyLen := int(binary.LittleEndian.Uint16(block[off : off+2]))
	valLen := int(binary.LittleEndian.Uint32(block[off+2 : off+6]))
	tomb := block[off+6] != 0
	ts := binary.LittleEndian.Uint64(block[off+7 : off+15])
	off += entryHeaderSize

	end := off + keyLen + valLen
	if end > len(block) {
		it.err = fmt.Errorf("sstable_iterator: truncated entry payload")
		it.key = nil
		return
	}

	it.key = block[off : off+keyLen]
	if valLen > 0 {
		it.value = block[off+keyLen : end]
	} else {
		it.value = nil
	}
	it.timestamp = ts
	it.tombstone = tomb

	it.blockOff = end
	it.entriesRead++
}

// Key returns the current key.
func (it *SSTableIterator) Key() []byte {
	return it.key
}

// Value returns the current value.
func (it *SSTableIterator) Value() []byte {
	return it.value
}

// Timestamp returns the current timestamp.
func (it *SSTableIterator) Timestamp() uint64 {
	return it.timestamp
}

// Tombstone returns true if the entry is a deletion marker.
func (it *SSTableIterator) Tombstone() bool {
	return it.tombstone
}

// Error returns any error encountered during iteration.
func (it *SSTableIterator) Error() error {
	return it.err
}

// Close releases resources.
func (it *SSTableIterator) Close() error {
	return nil // file is managed by SSTableReader
}
