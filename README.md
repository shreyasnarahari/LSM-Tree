# LSM-Tree: Log-Structured Merge Tree Storage Engine

A **production-grade LSM Tree key-value storage engine** built from scratch in Go using **zero external dependencies** — only the Go standard library.

This project is a rigorous systems engineering exercise designed to deeply explore direct disk I/O, binary serialization, lock-free data structures, and safe background daemon processing.

---

## Architecture

```
                          ┌──────────────────────────────────────────┐
                          │              LSM-Tree Engine             │
                          │                                          │
  Client ──Put(k,v)──►    │  ┌───────┐    ┌────────────┐            │
                          │  │  WAL  │───►│  MemTable   │            │
                          │  │(disk) │    │ (Skip List) │            │
                          │  └───────┘    └──────┬─────┘            │
                          │                      │ (threshold)       │
  Client ──Get(k)──►      │                      ▼                  │
                          │              ┌───────────────┐          │
                          │              │  Flush to SST  │          │
                          │              └───────┬───────┘          │
                          │                      ▼                  │
                          │  ┌──────────────────────────────────┐   │
                          │  │     Sorted String Tables (L0)    │   │
                          │  │  ┌───────┐ ┌───────┐ ┌───────┐  │   │
                          │  │  │SST #3 │ │SST #2 │ │SST #1 │  │   │
                          │  │  └───────┘ └───────┘ └───────┘  │   │
                          │  └──────────────────────────────────┘   │
                          └──────────────────────────────────────────┘
```

### Write Path
```
Put(key, value)
  │
  ├── 1. Append to WAL (sequential write, buffered + fsync)
  │
  └── 2. Insert into MemTable (in-memory skip list, O(log N))
         │
         └── (if MemTable exceeds threshold)
               ├── Rotate: active → immutable
               ├── Create fresh MemTable
               └── Background flush immutable → SSTable on disk
```

### Read Path
```
Get(key) → search in freshness order, stop at first match:
  │
  ├── 1. Active MemTable           (in-memory, O(log N))
  ├── 2. Immutable MemTables       (newest → oldest)
  └── 3. L0 SSTables               (newest → oldest)
           │
           ├── Bloom Filter check   → reject absent keys (0 disk I/O)
           ├── Binary search index  → locate 4KB data block
           └── Read single block    → linear scan for key
```

---

## Components (Implemented)

### Phase 1 — Write-Ahead Log (WAL)

The durability backbone. Every mutation is persisted to the WAL **before** being acknowledged to the client.

| Feature | Detail |
|---------|--------|
| **Binary format** | `[CRC32 4B][Timestamp 8B][Tombstone 1B][KeySize 2B][ValSize 4B][Key][Value]` |
| **Checksum** | CRC-32C (Castagnoli) — hardware-accelerated on x86-64 (SSE 4.2) and ARM |
| **Buffering** | `bufio.Writer` (4KB) batches writes; `os.File.Sync()` forces fsync |
| **Recovery** | Sequential replay with torn-write detection — truncates corrupt tail |
| **Append allocs** | **0 allocs/op** — pre-allocated scratch buffer, zero-copy key/value forwarding |

**Files:** [`wal.go`](wal.go) · [`wal_test.go`](wal_test.go)

---

### Phase 2 — MemTable (Skip List)

A probabilistically balanced in-memory sorted data structure for fast writes and reads.

| Feature | Detail |
|---------|--------|
| **Structure** | Skip List (maxHeight=32, p=0.25) |
| **Complexity** | O(log N) Put and Get |
| **Concurrency** | `sync.RWMutex` — concurrent readers, exclusive writers |
| **Memory tracking** | `atomic.Int64` — lock-free size monitoring for threshold detection |
| **Get allocs** | **0 allocs/op** — returns direct reference to internal slice |
| **Iterator** | Forward-only, sorted-order traversal for SSTable flushing |

**Files:** [`skiplist.go`](skiplist.go) · [`memtable.go`](memtable.go) · [`memtable_test.go`](memtable_test.go)

---

### Phase 3 — SSTables & Bloom Filters

Immutable, sorted files on disk optimized for minimal seek latency.

#### SSTable File Layout
```
┌─────────────────────────────────────────┐
│ Data Block 0  (4096 bytes, page-aligned)│
│   [NumEntries][Entries…][Zero-padding]  │
│ Data Block 1 … N                        │
├─────────────────────────────────────────┤
│ Index Block                             │
│   [StartKey → (Offset, Length)] × N     │
├─────────────────────────────────────────┤
│ Bloom Filter Block                      │
│   [NumBits | NumHashes | BitArray]      │
├─────────────────────────────────────────┤
│ Footer (40 bytes)                       │
│   IndexOffset | IndexSize | BloomOffset │
│   BloomSize   | Magic (0x4C534D5401)   │
└─────────────────────────────────────────┘
```

#### Bloom Filter
- **Algorithm:** Kirsch-Mitzenmacker double hashing (FNV-1a 64-bit → two 32-bit halves)
- **Target:** 1% false-positive rate → measured **1.00%** ✓
- **Rejection:** **99.3%** of absent keys rejected without any disk I/O

| Operation | Latency | Allocations |
|-----------|---------|-------------|
| Get (hit) | 2.2 µs | 2 allocs (block + value copy) |
| Get (miss) | 65 ns | 0 allocs (bloom rejects) |

**Files:** [`bloom.go`](bloom.go) · [`sstable_builder.go`](sstable_builder.go) · [`sstable_reader.go`](sstable_reader.go) · [`sstable_test.go`](sstable_test.go)

---

## Upcoming Phases

| Phase | Component | Status |
|-------|-----------|--------|
| 4 | **Core Engine** — DB struct, concurrent read/write paths, MemTable rotation, background flush | 🔜 Next |
| 5 | **Background Compaction** — k-way merge via min-heap, tombstone purging, atomic file swap | 📋 Planned |

---

## Getting Started

### Prerequisites

- **Go 1.22+** (no external dependencies)

### Run Tests

```bash
# Run all unit tests with verbose output
go test -v -count=1 ./...

# Run with race detector (recommended)
go test -v -count=1 -race ./...
```

### Run Benchmarks

```bash
# All benchmarks with memory allocation tracking
go test -bench=. -benchmem -benchtime=3s -run=^$ ./...

# WAL-specific benchmarks
go test -bench=BenchmarkWAL -benchmem -run=^$ ./...

# Skip List / MemTable benchmarks
go test -bench='BenchmarkSkipList|BenchmarkMemTable' -benchmem -run=^$ ./...

# SSTable and Bloom filter benchmarks
go test -bench='BenchmarkSSTable|BenchmarkBloom' -benchmem -run=^$ ./...
```

### Escape Analysis Audit

Verify that hot-path functions avoid unexpected heap allocations:

```bash
go build -gcflags="-m" ./... 2>&1 | grep -E "(wal|skiplist|memtable|bloom|sstable)"
```

### Inspect SSTable Binary Layout

```bash
# Build an SSTable via tests, then inspect with hexdump
go test -run TestSSTableRoundTrip -v ./...
hexdump -C /tmp/test-sstable/*.sst | head -80
```

---

## Benchmark Results

All benchmarks run on **Intel Core i5-10300H @ 2.50GHz**, Linux (WSL2).

### WAL
```
BenchmarkWALAppend-8          23,161,273    166.3 ns/op     0 B/op    0 allocs/op
BenchmarkWALAppendBatch-8          1,136  3,279,321 ns/op  29.0 MB/s  0 B/op    0 allocs/op
BenchmarkWALReplay-8                 284 12,792,319 ns/op 523.8 MB/s
```

### MemTable / Skip List
```
BenchmarkSkipListGet-8        14,491,893    162.4 ns/op     0 B/op    0 allocs/op
BenchmarkSkipListGetParallel  45,977,524     51.2 ns/op     0 B/op    0 allocs/op
BenchmarkMemTablePut-8        20,504,230    108.1 ns/op    48 B/op    1 allocs/op
BenchmarkMemTableGet-8        13,374,375    157.9 ns/op     0 B/op    0 allocs/op
```

### SSTable & Bloom Filter
```
BenchmarkBloomFilterAdd-8     53,940,720     43.9 ns/op     0 B/op    0 allocs/op
BenchmarkBloomFilterQuery-8   45,978,478     47.5 ns/op     0 B/op    0 allocs/op
BenchmarkSSTableGet-8            972,285   2193   ns/op  4112 B/op    2 allocs/op
BenchmarkSSTableGetMiss-8     36,227,282     64.7 ns/op    49 B/op    0 allocs/op
```

---

## Design Constraints

- **Zero external dependencies** — only the Go standard library
- **Zero-allocation read paths** — verified via `-benchmem` and escape analysis
- **Explicit error handling** — wrapped errors (`fmt.Errorf("...: %w", err)`), no panics
- **Endianness** — `binary.LittleEndian` throughout (matches x86/ARM LE architectures)
- **Durability** — WAL with CRC-32C checksums + fsync guarantees
- **Concurrency** — `sync.RWMutex` for reader-writer separation, `sync/atomic` for counters

---

## Project Structure

```
LSM-Tree/
├── go.mod                 # Module: lsmtree (Go 1.22, zero deps)
├── wal.go                 # Write-Ahead Log (Phase 1)
├── wal_test.go            # 12 tests + 3 benchmarks
├── skiplist.go            # Skip List data structure (Phase 2)
├── memtable.go            # MemTable wrapper + iterator (Phase 2)
├── memtable_test.go       # 14 tests + 8 benchmarks
├── bloom.go               # Bloom Filter (Phase 3)
├── sstable_builder.go     # SSTable writer (Phase 3)
├── sstable_reader.go      # SSTable reader (Phase 3)
├── sstable_test.go        # 11 tests + 5 benchmarks
└── README.md              # This file
```

---

## License

Educational project — no license specified.
