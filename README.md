# LSM-Tree: Log-Structured Merge Tree Storage Engine

A **production-grade LSM Tree key-value storage engine** built from scratch in Go using **zero external dependencies** — only the Go standard library.

This project is a rigorous systems engineering exercise designed to deeply explore direct disk I/O, binary serialization, concurrent data structures, and safe background daemon processing.

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
  ├── 1. Append to WAL (sequential write, buffered + fsync for durability)
  │
  └── 2. Insert into MemTable (in-memory skip list, O(log N))
         │
         └── (if MemTable exceeds threshold)
               ├── Sync WAL → rotate: active MemTable → immutable
               ├── Create fresh MemTable + new WAL
               └── Signal background goroutine → flush immutable → SSTable
```

### Read Path
```
Get(key) → search in freshness order, stop at first hit or tombstone:
  │
  ├── 1. Active MemTable           (in-memory, O(log N), 0 allocs)
  ├── 2. Immutable MemTables       (newest → oldest)
  └── 3. L0 SSTables               (newest → oldest)
           │
           ├── Check MinKey bounds  → reject if key is smaller (0 disk I/O)
           ├── Binary search index  → locate 4KB data block
           └── Read single block    → linear scan for key
```

> **Tombstone short-circuit:** If a deletion marker (tombstone) is found at *any* level, `ErrKeyNotFound` is returned immediately — no deeper levels are searched. This guarantees O(1) delete semantics across the read path.

---

## Components

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

### Phase 3 — SSTables

Immutable, sorted files on disk optimized for minimal seek latency.

#### SSTable File Layout
```
┌─────────────────────────────────────────┐
│ Data Block 0  (4096 bytes, page-aligned) │
│   [NumEntries][Entries…][Zero-padding]   │
│ Data Block 1 … N                         │
├──────────────────────────────────────────┤
│ Index Block                              │
│   [StartKey → (Offset, Length)] × N      │
├──────────────────────────────────────────┤
│ Footer (24 bytes)                        │
│   IndexOffset | IndexSize                │
│   Magic (0x4C534D5401)                   │
└──────────────────────────────────────────┘
```

**Files:** [`sstable_builder.go`](sstable_builder.go) · [`sstable_reader.go`](sstable_reader.go) · [`sstable_test.go`](sstable_test.go)

---

### Phase 4 — Core Engine

The central orchestrator that ties all components into a thread-safe `DB` struct.

| Feature | Detail |
|---------|--------|
| **Write path** | `sync.Mutex` serialised: WAL append → MemTable insert → rotate if full |
| **Read path** | `sync.RWMutex` concurrent: Active → Immutables → SSTables (newest first) |
| **MemTable rotation** | Brief pointer swap under write lock; background goroutine flushes to SSTable |
| **Background flush** | Dedicated goroutine with `context.Context` cancellation + `sync.WaitGroup` lifecycle |
| **Crash recovery** | On startup: load existing SSTables + replay WAL to reconstruct MemTable |
| **Graceful shutdown** | `Close()` cancels context → waits for flush goroutine → syncs WAL → closes files |
| **Tombstone shadowing** | Delete at any level immediately returns `ErrKeyNotFound` — no deeper search |

#### Concurrency Model
```
                       ┌────────────────┐
   Client writes ────► │  sync.Mutex    │──► WAL.Append() ──► MemTable.Put()
                       └────────────────┘
                              │
                       (MemTable full?)
                              │ yes
                       ┌──────▼──────┐
                       │   Rotate    │  stateMu.Lock: swap active ↔ immutable
                       └──────┬──────┘
                              │
                       ┌──────▼──────┐
                       │  flushCh    │──► Background Goroutine
                       └─────────────┘         │
                                               ▼
                                       BuildSSTable() → Add to L0

   Client reads ─────► stateMu.RLock → search active → imm → ssts → RUnlock
```

**Files:** [`db.go`](db.go) · [`db_test.go`](db_test.go)

---

### Phase 5 — Background Compaction

To prevent read performance degradation, a background goroutine merges multiple SSTables into a single SSTable, purging tombstones and reclaiming disk space.

| Feature | Detail |
|---------|--------|
| **Strategy** | Universal L0 Compaction (triggered by `CompactionThreshold`) |
| **K-Way Merge** | `container/heap` min-heap across iterators |
| **Shadowing** | Newer iterators win; older occurrences of the same key are discarded |
| **Purging** | Tombstones are safely discarded when all files are compacted |
| **Atomic Swap** | New SSTable written → `stateMu` locked → old files removed and new file appended → lock released |

**Files:** [`iterator.go`](iterator.go) · [`sstable_iterator.go`](sstable_iterator.go) · [`merge_iterator.go`](merge_iterator.go) · [`compaction.go`](compaction.go) · [`compaction_test.go`](compaction_test.go)

---

## Upcoming

| Phase | Component | Status |
|-------|-----------|--------|
| 6 | **Next Steps** — Maybe a crash recovery stress test, caching (Block Cache), or network API (gRPC/HTTP) | 📋 TBD |

---

## Getting Started

### Prerequisites

- **Go 1.22+** (no external dependencies)

### Quick Start

```go
package main

import (
    "fmt"
    "log"
    "github.com/shreyas/lsmtree/db"
)

func main() {
    database, err := db.Open(db.DBOptions{
        Dir:          "./data",
        MemTableSize: 4 * 1024 * 1024, // 4 MB
        SyncOnWrite:  true,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer database.Close()

    // Write
    database.Put([]byte("hello"), []byte("world"))

    // Read
    val, err := database.Get([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("hello = %s\n", val)

    // Delete
    database.Delete([]byte("hello"))
}
```

### Run Tests

```bash
# All tests with race detector
go test -v -count=1 -race -timeout 120s ./...

# Phase-specific tests
go test -v -run TestWAL ./...        # Phase 1: WAL
go test -v -run 'TestSkip|TestMem' ./...  # Phase 2: MemTable
go test -v -run 'TestSST' ./...  # Phase 3: SSTable
go test -v -run TestDB ./...          # Phase 4: Engine
```

### Run Benchmarks

```bash
# All benchmarks with memory profiling
go test -bench=. -benchmem -benchtime=3s -run=^$ ./...

# Component-specific benchmarks
go test -bench=BenchmarkWAL -benchmem -run=^$ ./...
go test -bench=BenchmarkSkipList -benchmem -run=^$ ./...
go test -bench=BenchmarkDB -benchmem -run=^$ ./...
```

### Escape Analysis

```bash
go build -gcflags="-m" ./... 2>&1 | grep -v "test"
```

---


## Design Constraints

- **Zero external dependencies** — only the Go standard library
- **Zero-allocation read paths** — `DB.Get` from MemTable: 0 allocs/op
- **Explicit error handling** — wrapped errors (`fmt.Errorf("...: %w", err)`), no panics
- **Endianness** — `binary.LittleEndian` throughout (matches x86/ARM LE)
- **Durability** — WAL with CRC-32C checksums + fsync
- **Concurrency** — `sync.Mutex` for writes, `sync.RWMutex` for state, `sync/atomic` for counters
- **Goroutine safety** — `context.Context` cancellation + `sync.WaitGroup` for zero goroutine leaks

---

## Project Structure

```
LSM-Tree/
├── go.mod                 # Module: lsmtree (Go 1.22, zero deps)

├── compaction/            # Background compactions & min-heap merging (Phase 6)
├── db/                    # Core engine facade & orchestration (Phase 4 & 7)
├── internal/              # Core interfaces & binary encoding (Phase 1)
├── iterator/              # Universal iterator interfaces (Phase 1)
├── memtable/              # MemTable & Skip List (Phase 2)
├── sstable/               # SSTable readers and builders (Phase 3)
├── wal/                   # Write-Ahead Log (Phase 1)
├── cmd/
│   └── lsm/
│       └── main.go        # REPL CLI interface (Phase 8)
└── README.md              # This file
```

**Total: 48 tests, 20 benchmarks**, all passing with `-race` detector.

---

## License

Educational project — no license specified.
