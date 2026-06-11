package lsmtree

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure that tests
// whether a key is a member of a set. False positives are possible, but
// false negatives are not.
//
// This implementation uses Kirsch-Mitzenmacker double hashing: a single
// FNV-1a 64-bit hash is split into two 32-bit halves (h1, h2) and k
// hash positions are derived as pos_i = (h1 + i*h2) % m.
type BloomFilter struct {
	bits      []byte
	numBits   uint32
	numHashes uint8
}

// NewBloomFilter creates a filter sized for expectedItems at the given
// false-positive rate.
//
// Optimal parameters:
//
//	m = -n * ln(p) / (ln2)^2   (total bits)
//	k = (m/n) * ln2            (hash function count)
func NewBloomFilter(expectedItems int, fpRate float64) *BloomFilter {
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
	return &BloomFilter{
		bits:      make([]byte, (numBits+7)/8),
		numBits:   numBits,
		numHashes: uint8(k),
	}
}

// Add inserts key into the filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < uint32(bf.numHashes); i++ {
		pos := (h1 + i*h2) % bf.numBits
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// MayContain returns true if the key might be in the set, or false if
// the key is definitely not in the set.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bf.hash(key)
	for i := uint32(0); i < uint32(bf.numHashes); i++ {
		pos := (h1 + i*h2) % bf.numBits
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// hash returns two 32-bit halves of a 64-bit FNV-1a hash.
func (bf *BloomFilter) hash(key []byte) (uint32, uint32) {
	h := fnv.New64a()
	_, _ = h.Write(key)
	sum := h.Sum64()
	return uint32(sum), uint32(sum >> 32)
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

// bloomHeaderSize is [NumBits uint32][NumHashes uint8] = 5 bytes.
const bloomHeaderSize = 5

// MarshalBinary serialises the filter to bytes:
//
//	[NumBits 4B][NumHashes 1B][BitArray …]
func (bf *BloomFilter) MarshalBinary() []byte {
	buf := make([]byte, bloomHeaderSize+len(bf.bits))
	binary.LittleEndian.PutUint32(buf[0:4], bf.numBits)
	buf[4] = bf.numHashes
	copy(buf[bloomHeaderSize:], bf.bits)
	return buf
}

// UnmarshalBloomFilter reconstructs a filter from serialised bytes.
func UnmarshalBloomFilter(data []byte) *BloomFilter {
	numBits := binary.LittleEndian.Uint32(data[0:4])
	numHashes := data[4]
	bits := make([]byte, len(data)-bloomHeaderSize)
	copy(bits, data[bloomHeaderSize:])
	return &BloomFilter{
		bits:      bits,
		numBits:   numBits,
		numHashes: numHashes,
	}
}
