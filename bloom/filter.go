// Package bloom implements a space-efficient probabilistic data structure
// for set membership testing.
//
// The Bloom filter is used by SSTable readers to skip expensive disk I/O
// when a key is definitely not present. False positives are possible, but
// false negatives are not.
//
// This implementation uses Kirsch-Mitzenmacker double hashing: a single
// FNV-1a 64-bit hash is split into two 32-bit halves (h1, h2) and k
// hash positions are derived as pos_i = (h1 + i*h2) % m.
package bloom

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// headerSize is the serialised header: [NumBits uint32][NumHashes uint8] = 5 bytes.
const headerSize = 5

// Filter is a space-efficient probabilistic data structure that tests
// whether a key is a member of a set. False positives are possible, but
// false negatives are not.
type Filter struct {
	bits      []byte
	NumBits   uint32
	NumHashes uint8
}

// New creates a filter sized for expectedItems at the given
// false-positive rate.
//
// Optimal parameters:
//
//	m = -n * ln(p) / (ln2)^2   (total bits)
//	k = (m/n) * ln2            (hash function count)
func New(expectedItems int, fpRate float64) *Filter {
	if expectedItems < 1 {
		expectedItems = 1
	}
	n := float64(expectedItems)
	// m = -n * ln(p) / (ln2)^2
	m := math.Ceil(-n * math.Log(fpRate) / (math.Ln2 * math.Ln2))
	if m < 8 {
		m = 8
	}
	// k = (m/n) * ln2
	k := math.Ceil(m / n * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	numBits := uint32(m)
	return &Filter{
		bits:      make([]byte, (numBits+7)/8),
		NumBits:   numBits,
		NumHashes: uint8(k),
	}
}

// Add inserts key into the filter.
func (f *Filter) Add(key []byte) {
	h1, h2 := f.hash(key)
	for i := uint32(0); i < uint32(f.NumHashes); i++ {
		pos := (h1 + i*h2) % f.NumBits
		f.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the key might be in the set, or false if
// the key is definitely not in the set.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := f.hash(key)
	for i := uint32(0); i < uint32(f.NumHashes); i++ {
		pos := (h1 + i*h2) % f.NumBits
		if f.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// hash returns two 32-bit halves of a 64-bit FNV-1a hash.
func (f *Filter) hash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	_, _ = h.Write(key)
	sum := h.Sum64()
	return uint32(sum), uint32(sum >> 32)
}

// Serialization

// MarshalBinary serialises the filter to bytes:
//
//	[NumBits 4B][NumHashes 1B][BitArray …]
func (f *Filter) MarshalBinary() []byte {
	buf := make([]byte, headerSize+len(f.bits))
	binary.LittleEndian.PutUint32(buf[0:4], f.NumBits)
	buf[4] = f.NumHashes
	copy(buf[headerSize:], f.bits)
	return buf
}

// Unmarshal reconstructs a filter from serialised bytes.
func Unmarshal(data []byte) *Filter {
	numBits := binary.LittleEndian.Uint32(data[0:4])
	numHashes := data[4]
	bits := make([]byte, len(data)-headerSize)
	copy(bits, data[headerSize:])
	return &Filter{
		bits:      bits,
		NumBits:   numBits,
		NumHashes: numHashes,
	}
}
