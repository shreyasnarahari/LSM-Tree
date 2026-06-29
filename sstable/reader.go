package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/shreyas/lsmtree/bloom"
)

// Reader provides read-only access to an immutable SSTable file.
//
// On Open it loads the index block and bloom filter into memory. Point
// lookups follow the cascade:
//
//  1. Bloom filter — reject absent keys with zero disk I/O.
//  2. Binary search the in-memory index — locate the candidate data block.
//  3. Single Seek + Read of one 4 KB data block from disk.
//  4. Linear scan within the block to find the key.
type Reader struct {
	file  *os.File
	index []indexEntry
	bloom *bloom.Filter
	path  string
}

// Open opens an existing SSTable file, reads the footer, and
// loads the index and bloom filter into memory.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable: open %q: %w", path, err)
	}

	// Read footer (last 40 bytes).
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: stat %q: %w", path, err)
	}
	if fi.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable: file too small (%d bytes)", fi.Size())
	}

	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], fi.Size()-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read footer: %w", err)
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexSize := binary.LittleEndian.Uint64(footer[8:16])
	bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
	bloomSize := binary.LittleEndian.Uint64(footer[24:32])
	magic := binary.LittleEndian.Uint64(footer[32:40])

	if magic != sstMagic {
		f.Close()
		return nil, fmt.Errorf("sstable: bad magic 0x%X, want 0x%X", magic, sstMagic)
	}

	// Read index block.
	indexBuf := make([]byte, indexSize)
	if _, err := f.ReadAt(indexBuf, int64(indexOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read index: %w", err)
	}
	index, err := unmarshalIndex(indexBuf)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: parse index: %w", err)
	}

	// Read bloom filter block.
	bloomBuf := make([]byte, bloomSize)
	if _, err := f.ReadAt(bloomBuf, int64(bloomOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable: read bloom: %w", err)
	}
	bf := bloom.Unmarshal(bloomBuf)

	return &Reader{
		file:  f,
		index: index,
		bloom: bf,
		path:  path,
	}, nil
}

// Get looks up key in the SSTable.
//
// Returns:
//   - (value, true, false, nil)  — key found, live entry
//   - (nil,   true, true,  nil)  — key found, tombstone
//   - (nil,   false, false, nil) — key not present
//   - (_,     _,    _,     err)  — I/O error
func (r *Reader) Get(key []byte) (value []byte, found bool, tombstone bool, err error) {
	// 1. Bloom filter: definite negative means skip disk entirely.
	if !r.bloom.MayContain(key) {
		return nil, false, false, nil
	}

	// 2. Binary search the index to find the candidate data block.
	//    Find the last block whose startKey <= key.
	idx := sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].startKey, key) > 0
	}) - 1

	if idx < 0 {
		// Key is smaller than the first key in the SSTable.
		return nil, false, false, nil
	}

	// 3. Read the single candidate data block from disk.
	entry := r.index[idx]
	block := make([]byte, entry.length)
	if _, err := r.file.ReadAt(block, int64(entry.offset)); err != nil && err != io.EOF {
		return nil, false, false, fmt.Errorf("sstable: read block at %d: %w", entry.offset, err)
	}

	// 4. Linear scan within the block.
	return scanBlock(block, key)
}

// scanBlock parses entries within a data block and looks for key.
func scanBlock(block []byte, key []byte) (value []byte, found bool, tombstone bool, err error) {
	if len(block) < blockHeaderSize {
		return nil, false, false, fmt.Errorf("sstable: block too small")
	}
	numEntries := binary.LittleEndian.Uint32(block[0:blockHeaderSize])
	off := blockHeaderSize

	for i := uint32(0); i < numEntries; i++ {
		if off+entryHeaderSize > len(block) {
			return nil, false, false, fmt.Errorf("sstable: truncated entry header at %d", off)
		}
		keyLen := int(binary.LittleEndian.Uint16(block[off : off+2]))
		valLen := int(binary.LittleEndian.Uint32(block[off+2 : off+6]))
		tomb := block[off+6] != 0
		// timestamp at block[off+7 : off+15] — not needed for point lookup
		off += entryHeaderSize

		end := off + keyLen + valLen
		if end > len(block) {
			return nil, false, false, fmt.Errorf("sstable: truncated entry payload at %d", off)
		}

		eKey := block[off : off+keyLen]
		cmp := bytes.Compare(eKey, key)
		if cmp == 0 {
			var val []byte
			if valLen > 0 {
				val = make([]byte, valLen)
				copy(val, block[off+keyLen:off+keyLen+valLen])
			}
			return val, true, tomb, nil
		}
		if cmp > 0 {
			// Keys are sorted; no point continuing.
			break
		}
		off += keyLen + valLen
	}
	return nil, false, false, nil
}

func (r *Reader) MinKey() []byte {
	if len(r.index) == 0 {
		return nil
	}
	return r.index[0].startKey
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) Path() string {
	return r.path
}

func (r *Reader) BlockCount() int {
	return len(r.index)
}

// Index deserialization

func unmarshalIndex(data []byte) ([]indexEntry, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("index too short")
	}
	n := int(binary.LittleEndian.Uint32(data[0:4]))
	off := 4
	entries := make([]indexEntry, 0, n)

	for i := 0; i < n; i++ {
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated index entry %d", i)
		}
		keyLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+keyLen+16 > len(data) {
			return nil, fmt.Errorf("truncated index entry %d payload", i)
		}
		startKey := make([]byte, keyLen)
		copy(startKey, data[off:off+keyLen])
		off += keyLen
		offset := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		length := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8

		entries = append(entries, indexEntry{
			startKey: startKey,
			offset:   offset,
			length:   length,
		})
	}
	return entries, nil
}
