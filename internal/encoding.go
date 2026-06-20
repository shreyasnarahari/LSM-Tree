package internal

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Binary wire format for an Entry:
//
//	┌───────────┬───────────┬────────────┬────────┬─────────┬───────────┐
//	│ KeyLen(2) │ ValLen(4) │ Timestamp(8)│  Op(1) │ Key(…)  │ Value(…)  │
//	└───────────┴───────────┴────────────┴────────┴─────────┴───────────┘
//
// Total header: 2 + 4 + 8 + 1 = 15 bytes (fixed), followed by variable
// key and value payloads.
//
// All multi-byte integers are encoded in LittleEndian to match the
// existing WAL and SSTable formats in this project.

const (
	// EntryHeaderSize is the fixed byte count of the entry wire header.
	EntryHeaderSize = 2 + 4 + 8 + 1 // 15 bytes

	// MaxKeySize is the maximum key length (uint16 max).
	MaxKeySize = 1<<16 - 1 // 65,535

	// MaxValueSize is the maximum value length (uint32 max).
	MaxValueSize = 1<<32 - 1 // 4,294,967,295
)

// Encode serialises an Entry into the binary wire format and writes it
// to w. The header is assembled in a stack-allocated array to avoid
// heap allocations on the hot path.
//
// Returns an error if the key or value exceeds format limits, or if
// the underlying writer fails.
func Encode(e *Entry, w io.Writer) error {
	if len(e.Key) > MaxKeySize {
		return fmt.Errorf("internal: key length %d exceeds max %d", len(e.Key), MaxKeySize)
	}
	if len(e.Value) > MaxValueSize {
		return fmt.Errorf("internal: value length %d exceeds max %d", len(e.Value), MaxValueSize)
	}

	var hdr [EntryHeaderSize]byte
	binary.LittleEndian.PutUint16(hdr[0:2], uint16(len(e.Key)))
	binary.LittleEndian.PutUint32(hdr[2:6], uint32(len(e.Value)))
	binary.LittleEndian.PutUint64(hdr[6:14], e.Timestamp)
	hdr[14] = byte(e.Op)

	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("internal: write header: %w", err)
	}
	if _, err := w.Write(e.Key); err != nil {
		return fmt.Errorf("internal: write key: %w", err)
	}
	if len(e.Value) > 0 {
		if _, err := w.Write(e.Value); err != nil {
			return fmt.Errorf("internal: write value: %w", err)
		}
	}
	return nil
}

// Decode reads a single Entry from r in the binary wire format.
//
// Returns (nil, io.EOF) on a clean end-of-stream, or (nil, io.ErrUnexpectedEOF)
// if the stream is truncated mid-record. Key and value slices are freshly
// allocated and safe to retain.
func Decode(r io.Reader) (*Entry, error) {
	var hdr [EntryHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, err
		}
		return nil, fmt.Errorf("internal: read header: %w", err)
	}

	keyLen := int(binary.LittleEndian.Uint16(hdr[0:2]))
	valLen := int(binary.LittleEndian.Uint32(hdr[2:6]))
	timestamp := binary.LittleEndian.Uint64(hdr[6:14])
	op := OpType(hdr[14])

	payload := make([]byte, keyLen+valLen)
	if len(payload) > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return nil, fmt.Errorf("internal: read payload: %w", err)
		}
	}

	key := payload[:keyLen]

	var value []byte
	if valLen > 0 {
		value = payload[keyLen:]
	}

	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		Op:        op,
	}, nil
}
