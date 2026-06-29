package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

const (
	// HeaderSize is the fixed byte count of every WAL record header.
	//
	// Layout:
	//   CRC32     (4 bytes, uint32 LE)
	//   Timestamp (8 bytes, uint64 LE)
	//   Tombstone (1 byte,  0x00 = live, 0x01 = deleted)
	//   KeySize   (2 bytes, uint16 LE)
	//   ValueSize (4 bytes, uint32 LE)
	//   ─────────────────────────────
	//   Total     19 bytes
	HeaderSize = 4 + 8 + 1 + 2 + 4 // 19

	// bufSize is the user-space buffer size for bufio.Writer.
	// Matches a typical OS page to amortise syscall overhead.
	bufSize = 4096

	// MaxKeySize is the format-imposed upper bound (uint16).
	MaxKeySize = 1<<16 - 1 // 65,535

	// MaxValueSize is the format-imposed upper bound (uint32).
	MaxValueSize = 1<<32 - 1 // 4,294,967,295
)

// crc32cTable is the pre-computed Castagnoli CRC-32C table.
// CRC-32C benefits from hardware acceleration on modern CPUs
// (SSE 4.2 on x86-64, CRC extensions on ARM).
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Entry represents a single record read back from the WAL during
// recovery replay.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
	Tombstone bool
}

// WAL is an append-only Write-Ahead Log that provides strict durability.
//
// Every key-value mutation is first serialised into a self-describing binary
// record and written to the WAL before the engine acknowledges the write
// to the caller. On crash recovery the log is replayed sequentially to
// reconstruct the volatile in-memory state (MemTable).
type WAL struct {
	file   *os.File
	writer *bufio.Writer
	path   string

	// scratch is a fixed-size header buffer embedded in the struct.
	// It is reused across every Append call, keeping the hot path
	// pre-allocated slice to serialize headers without allocations.
	scratch [HeaderSize]byte
}

// Open creates or opens an existing WAL file at path.
//
// The file is opened with O_APPEND so that every write(2) atomically
// seeks to the end before writing — a guarantee provided by POSIX for
// regular files. This means a separate read-only handle is used for
// recovery (see NewIterator).
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriterSize(f, bufSize),
		path:   path,
	}, nil
}

// Append serialises a single key-value mutation and writes it to the WAL.
//
// Binary record layout (LittleEndian throughout):
//
//	[CRC32 4B][Timestamp 8B][Tombstone 1B][KeySize 2B][ValueSize 4B][Key …][Value …]
//
// The CRC-32C checksum covers every byte after the CRC field itself
// (timestamp through the last value byte).
//
// This method does NOT call fsync; call Sync() to guarantee durability.
//
// The fixed header is written into the pre-allocated scratch array,
// and key/value bytes are forwarded directly from the caller's slices.
func (w *WAL) Append(key, value []byte, tombstone bool) error {
	// format constraints
	if len(key) > MaxKeySize {
		return fmt.Errorf("wal: key length %d exceeds max %d", len(key), MaxKeySize)
	}
	if len(value) > MaxValueSize {
		return fmt.Errorf("wal: value length %d exceeds max %d", len(value), MaxValueSize)
	}

	// build header in scratch[4:19] (skip the CRC slot)
	binary.LittleEndian.PutUint64(w.scratch[4:12], uint64(time.Now().UnixNano()))

	if tombstone {
		w.scratch[12] = 1
	} else {
		w.scratch[12] = 0
	}

	binary.LittleEndian.PutUint16(w.scratch[13:15], uint16(len(key)))
	binary.LittleEndian.PutUint32(w.scratch[15:19], uint32(len(value)))

	// compute CRC-32C over header[4:19] + key + value
	crc := crc32.Update(0, crc32cTable, w.scratch[4:19])
	crc = crc32.Update(crc, crc32cTable, key)
	crc = crc32.Update(crc, crc32cTable, value)
	binary.LittleEndian.PutUint32(w.scratch[0:4], crc)

	// write header
	if _, err := w.writer.Write(w.scratch[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	// write key
	if _, err := w.writer.Write(key); err != nil {
		return fmt.Errorf("wal: write key: %w", err)
	}
	// write value
	if _, err := w.writer.Write(value); err != nil {
		return fmt.Errorf("wal: write value: %w", err)
	}
	return nil
}

// Sync flushes the user-space bufio buffer to the OS page cache
// and then forces the kernel to persist all dirty pages for this
// file descriptor to the physical storage medium via fsync(2).
//
// After Sync returns nil the data is guaranteed to survive power
// loss and kernel panics.
func (w *WAL) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

func (w *WAL) Close() error {
	syncErr := w.Sync()
	closeErr := w.file.Close()
	if syncErr != nil {
		return fmt.Errorf("wal: close: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("wal: close file: %w", closeErr)
	}
	return nil
}

// Size returns the logical size of the WAL, which is the physical file
// size plus any bytes sitting in the unflushed bufio buffer.
func (w *WAL) Size() (int64, error) {
	info, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal: stat: %w", err)
	}
	return info.Size() + int64(w.writer.Buffered()), nil
}

// Iterator – sequential recovery replay with torn-write detection

// Iterator reads WAL records sequentially for crash recovery.
//
// Each record's CRC-32C checksum is verified. On encountering a
// corrupt or partially-written (torn) record the iterator truncates
// the file at the last known-good boundary and signals EOF.
type Iterator struct {
	file   *os.File
	reader io.Reader
	offset int64 // byte offset of the next record to read

	// header is reused across Next() calls to avoid per-record allocation.
	header [HeaderSize]byte
}

// NewIterator opens the WAL file at path for recovery replay.
//
// The file is opened in read-write mode so the iterator can truncate
// a corrupt tail if one is detected.
func NewIterator(path string) (*Iterator, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open iterator %q: %w", path, err)
	}
	return &Iterator{
		file:   f,
		reader: bufio.NewReaderSize(f, bufSize),
	}, nil
}

// Next reads and validates the next record from the WAL.
//
// Return contract:
//
//	(*Entry, nil)  — valid record
//	(nil, nil)     — clean EOF or truncated corrupt tail (no more data)
//	(nil, error)   — unrecoverable I/O error
//
// Torn-write handling:
//  1. Partial header  (< 19 bytes before EOF) → truncate at record start.
//  2. Partial payload (< keySize+valueSize)   → truncate at record start.
//  3. CRC-32C mismatch                        → truncate at record start.
//
// In all three cases the file is truncated to discard the corrupt tail,
// and (nil, nil) is returned to signal a clean stop.
func (it *Iterator) Next() (*Entry, error) {
	recordStart := it.offset

	// 1. read fixed header
	_, err := io.ReadFull(it.reader, it.header[:])
	if err == io.EOF {
		return nil, nil // clean EOF
	}
	if err == io.ErrUnexpectedEOF {
		// Torn header: fewer than 19 bytes remain.
		if e := it.truncateAt(recordStart); e != nil {
			return nil, fmt.Errorf("wal: truncate torn header at %d: %w", recordStart, e)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("wal: read header at %d: %w", recordStart, err)
	}

	// 2. parse header fields
	storedCRC := binary.LittleEndian.Uint32(it.header[0:4])
	timestamp := binary.LittleEndian.Uint64(it.header[4:12])
	tombstone := it.header[12] != 0
	keySize := int(binary.LittleEndian.Uint16(it.header[13:15]))
	valueSize := int(binary.LittleEndian.Uint32(it.header[15:19]))

	// 3. read variable-length payload
	payloadLen := keySize + valueSize
	payload := make([]byte, payloadLen)

	if payloadLen > 0 {
		_, err = io.ReadFull(it.reader, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if e := it.truncateAt(recordStart); e != nil {
				return nil, fmt.Errorf("wal: truncate torn payload at %d: %w", recordStart, e)
			}
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("wal: read payload at %d: %w", recordStart, err)
		}
	}

	// 4. verify CRC-32C
	computed := crc32.Update(0, crc32cTable, it.header[4:19])
	computed = crc32.Update(computed, crc32cTable, payload)
	if storedCRC != computed {
		if e := it.truncateAt(recordStart); e != nil {
			return nil, fmt.Errorf("wal: truncate bad crc at %d: %w", recordStart, e)
		}
		return nil, nil
	}

	// 5. advance offset past this valid record
	it.offset = recordStart + int64(HeaderSize) + int64(payloadLen)

	return &Entry{
		Key:       payload[:keySize],
		Value:     payload[keySize:],
		Timestamp: timestamp,
		Tombstone: tombstone,
	}, nil
}

// truncateAt removes all data from offset onwards, restoring the WAL
// to the last known-good state.
func (it *Iterator) truncateAt(offset int64) error {
	return it.file.Truncate(offset)
}

func (it *Iterator) Close() error {
	return it.file.Close()
}
