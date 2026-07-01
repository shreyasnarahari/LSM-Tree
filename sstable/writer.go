package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/shreyas/lsmtree/internal"
	"github.com/shreyas/lsmtree/iterator"
)

// on-disk constants
const (
	// blockSize is the target data block size, aligned to the OS page size
	// for optimal read-ahead and page-cache behaviour.
	blockSize = 4096

	// blockHeaderSize is the uint32 entry count at the start of each block.
	blockHeaderSize = 4

	// entryHeaderSize: KeyLen(2) + ValLen(4) + Tombstone(1) + Timestamp(8) = 15.
	entryHeaderSize = 15

	// footerSize: 3 × uint64 = 24 bytes.
	footerSize = 24

	// sstMagic is the magic number written in the SSTable footer.
	// Encodes "LSMT\x01" conceptually.
	sstMagic uint64 = 0x4C534D5401
)

// indexEntry (in-memory representation)
type indexEntry struct {
	startKey []byte
	offset   uint64
	length   uint64
}

// Build consumes all entries from iter (which must yield keys in
// strictly ascending lexicographic order) and writes a complete SSTable to
// the file at path.
//
// File layout:
//
//	[Data Block 0 (4 KB padded)]…[Data Block N]
//	[Index Block]
//	[Footer (24 B)]
//
// Each data block entry:
//
//	[KeyLen 2B][ValLen 4B][Tombstone 1B][Timestamp 8B][Key][Value]
func Build(path string, iter iterator.Iterator) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("sstable: create %q: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriterSize(f, 64*1024) // 64 KB write buffer

	var index []indexEntry
	var blockBuf bytes.Buffer
	var blockEntries uint32
	var blockFirstKey []byte
	fileOffset := int64(0)

	// flushBlock seals the current data block, pads it to blockSize, and
	// records its metadata in the index.
	flushBlock := func() error {
		if blockEntries == 0 {
			return nil
		}
		// Block = [NumEntries uint32][entries…][zero-padding to blockSize]
		var hdr [blockHeaderSize]byte
		binary.LittleEndian.PutUint32(hdr[:], blockEntries)

		dataLen := blockHeaderSize + blockBuf.Len()
		padLen := 0
		if dataLen < blockSize {
			padLen = blockSize - dataLen
		}

		if _, err := w.Write(hdr[:]); err != nil {
			return fmt.Errorf("sstable: write block header: %w", err)
		}
		if _, err := w.Write(blockBuf.Bytes()); err != nil {
			return fmt.Errorf("sstable: write block data: %w", err)
		}
		if padLen > 0 {
			pad := make([]byte, padLen)
			if _, err := w.Write(pad); err != nil {
				return fmt.Errorf("sstable: write block padding: %w", err)
			}
		}

		totalLen := dataLen + padLen
		index = append(index, indexEntry{
			startKey: internal.CloneBytes(blockFirstKey),
			offset:   uint64(fileOffset),
			length:   uint64(totalLen),
		})

		fileOffset += int64(totalLen)
		blockBuf.Reset()
		blockEntries = 0
		blockFirstKey = nil
		return nil
	}

	// write data blocks
	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()
		tombstone := iter.Tombstone()
		timestamp := iter.Timestamp()

		eSize := entryHeaderSize + len(key) + len(value)

		// Seal current block if adding this entry would exceed blockSize.
		if blockEntries > 0 && blockHeaderSize+blockBuf.Len()+eSize > blockSize {
			if err := flushBlock(); err != nil {
				return err
			}
		}

		if blockEntries == 0 {
			blockFirstKey = internal.CloneBytes(key)
		}

		// Write entry to block buffer.
		var ehdr [entryHeaderSize]byte
		binary.LittleEndian.PutUint16(ehdr[0:2], uint16(len(key)))
		binary.LittleEndian.PutUint32(ehdr[2:6], uint32(len(value)))
		if tombstone {
			ehdr[6] = 1
		}
		binary.LittleEndian.PutUint64(ehdr[7:15], timestamp)

		blockBuf.Write(ehdr[:])
		blockBuf.Write(key)
		blockBuf.Write(value)
		blockEntries++

		iter.Next()
	}

	// Flush remaining entries.
	if err := flushBlock(); err != nil {
		return err
	}

	// write index block
	indexOffset := fileOffset
	indexData := marshalIndex(index)
	if _, err := w.Write(indexData); err != nil {
		return fmt.Errorf("sstable: write index: %w", err)
	}
	fileOffset += int64(len(indexData))

	// write footer
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint64(footer[8:16], uint64(len(indexData)))
	binary.LittleEndian.PutUint64(footer[16:24], sstMagic)
	if _, err := w.Write(footer[:]); err != nil {
		return fmt.Errorf("sstable: write footer: %w", err)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("sstable: flush: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sstable: fsync: %w", err)
	}
	return nil
}

// marshalIndex serialises index entries:
//
//	[NumEntries uint32]
//	per entry: [KeyLen uint16][Key…][Offset uint64][Length uint64]
func marshalIndex(entries []indexEntry) []byte {
	size := 4 // NumEntries
	for _, e := range entries {
		size += 2 + len(e.startKey) + 8 + 8
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(entries)))
	off := 4
	for _, e := range entries {
		binary.LittleEndian.PutUint16(buf[off:off+2], uint16(len(e.startKey)))
		off += 2
		copy(buf[off:], e.startKey)
		off += len(e.startKey)
		binary.LittleEndian.PutUint64(buf[off:off+8], e.offset)
		off += 8
		binary.LittleEndian.PutUint64(buf[off:off+8], e.length)
		off += 8
	}
	return buf
}

// max helper
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
