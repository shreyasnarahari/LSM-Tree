# LSM-Tree Engine Architecture & Implementation Plan

## 1. Overall Architectural Vision

The objective is to build a production-grade, highly performant **Log-Structured Merge (LSM) Tree key-value storage engine** from scratch in Go. The system has zero external dependencies, relying entirely on the Go standard library to maintain maximum control over memory layout, binary encoding, and file I/O operations.

### Data Flow Overview
1. **Writes (Put/Delete):** 
   - A mutation is first appended to a durable **Write-Ahead Log (WAL)** to ensure strict durability against crashes.
   - It is then inserted into a volatile, in-memory **MemTable** (backed by a thread-safe probabilistic SkipList).
   - Once the MemTable reaches a configurable memory threshold (e.g., 4 MB), it is marked as immutable, and a new active MemTable is rotated in.
2. **Flushing:**
   - A background thread iterates over the immutable MemTable in strictly ascending key order and writes it to disk as an **SSTable (Sorted String Table)**. 
   - After flushing, the corresponding WAL segment is safely deleted.
3. **Reads (Get):**
   - The engine searches in descending order of recency: Active MemTable → Immutable MemTable → SSTables.
   - To prevent expensive disk I/O, SSTables utilize an in-memory **Bloom Filter** to fast-reject queries for missing keys.
   - If the bloom filter yields a potential match, the engine binary searches an in-memory **Block Index** to find the exact 4KB data block containing the key, resulting in at most *one* disk seek/read per query.
4. **Compaction:**
   - A background daemon continually monitors disk usage. It performs a k-way merge of older SSTables (using a min-heap merge iterator) to purge tombstones, eliminate overwritten data, and maintain read efficiency.

### Codebase Restructuring Goal
The project originally resided entirely within a single, flat package (`lsmtree`). This restructuring effort breaks the codebase down into a highly modular, decoupled architecture where each component (`wal`, `memtable`, `sstable`, `bloom`, `compaction`) manages its own state and dependencies. 

> [!WARNING]
> **CRITICAL CONTEXT FOR AGENTS:** 
> We are migrating phase-by-phase. We have actively copied logic from the root `.go` files into new subdirectory packages. **However, we have NOT deleted the old root `.go` files yet.** 
> For example, both `/sstable_builder.go` and `/sstable/writer.go` exist simultaneously. This is deliberate. The `db.go` and `compaction.go` components currently residing in the root package still depend on the old root-level types to compile. **Do not delete the old flat files until Phase 7**, at which point `db.go` will be fully refactored to use the new packages, and the root can be cleaned up.

---

## 2. Current Progress Summary

We are currently transitioning from **Phase 5** to **Phase 6**. Below is the detailed breakdown of what has been accomplished:

### Phase 1: Core Types & Iterator Interface ✅ (Done)
- **Concept:** Provide a universal data interchange format to prevent cyclic imports between isolated packages.
- **Implementation:**
  - Standardized the `internal.Entry` struct to represent all data.
  - Implemented 15-byte fixed-header binary encoding/decoding.
  - Defined `iterator.Iterator` interface (`Valid`, `Next`, `Key`, `Value`, `Timestamp`, `Tombstone`, `Error`), ensuring that MemTables, SSTables, and MergeIterators all expose identical traversal semantics.
  - Module updated to Go 1.26.4.

### Phase 2: MemTable Package ✅ (Done)
- **Concept:** The volatile staging area for incoming writes.
- **Implementation:** 
  - Extracted to `memtable/` package.
  - Contains `skiplist.go` — a thread-safe concurrent map with strict memory footprint tracking.
  - `memtable.MemTable` wrapper manages the skip list and enforces configured memory bounds (`threshold`). 
  - Exposes `memtable.NewIterator()` which guarantees compile-time compliance with `iterator.Iterator`.

### Phase 3: WAL Package ✅ (Done)
- **Concept:** Append-only log ensuring crash consistency.
- **Implementation:** 
  - Extracted to `wal/` package.
  - Uses a fixed 19-byte record header (CRC32, timestamp, tombstone, keysize, valuesize).
  - Integrates `bufio.Writer` for write batching and `os.File.Sync()` for POSIX-compliant fsyncs.
  - Iterator implements robust torn-write detection, truncating corrupt tails caused by power loss by evaluating CRC32c Castagnoli checksums.

### Phase 4: Bloom Filter Package ✅ (Done)
- **Concept:** Probabilistic space-efficient set to accelerate point lookups.
- **Implementation:**
  - Extracted to `bloom/` package.
  - Uses Kirsch-Mitzenmacher double hashing via standard `fnv-1a` to simulate `k` independent hash functions optimally.
  - Handles byte serialization with a 5-byte header (`NumBits`, `NumHashes`) ensuring seamless persistence into SSTables.

### Phase 5: SSTable Package ✅ (Done)
- **Concept:** The immutable on-disk storage format.
- **Implementation:**
  - Extracted to `sstable/` package.
  - **Write Path (`sstable.Build`):** Iterates over an incoming `iterator.Iterator`, sequentially appending 4KB zero-padded data blocks. Concludes by appending an Index Block, Bloom Filter Block, and a rigid 40-byte Footer.
  - **Read Path (`sstable.Open`):** Validates the magic number (`LSMT\x01`), loads the Footer, Bloom Filter, and Index array into RAM.
  - **Lookup (`Get`):** Short-circuits absent keys using the bloom filter, binary-searches the index array for the candidate block offset, reads exactly one 4KB block from disk, and linearly scans it.

---

## 3. Pending Tasks & Upcoming Phases

### Phase 6: Compaction Package ⏳ (Pending)
- **Goal:** Decouple the background maintenance process that merges files and purges stale data.
- **Tasks:**
  - **Merge Iterator (`compaction/merge_iterator.go`):** Port the `merge_iterator` logic from the root package. It must accept an array of `iterator.Iterator` interfaces and use a min-heap to yield keys in strict ascending order across all underlying files, naturally shadowing older entries.
  - **Compaction Daemon (`compaction/compaction.go`):** Decouple the compaction scheduling and execution logic from the `DB` struct. Define a clear interface or configuration struct to manage levels or size-tier triggers.
  - **Testing:** Migrate and expand `merge_iterator_test.go` into `compaction/`. Ensure rigorous tests for overlapping keys and tombstone purging.

### Phase 7: DB Engine Package ⏳ (Pending)
- **Goal:** The facade and core orchestration layer. Wire all isolated modules together.
- **Tasks:**
  - **Configuration:** Create `db/options.go` defining thresholds (MemTable max size, Compaction triggers, WAL sync intervals).
  - **Main Struct (`db/db.go`):** Port `db.go`. This object must manage the thread-safe concurrency model using `sync.RWMutex`, maintaining pointers to the `wal.WAL`, the active `memtable.MemTable`, immutable memtables, and the active `sstable.Reader` array.
  - **Recovery:** Implement the crash-recovery sequence that replays the WAL into a fresh MemTable on startup.
  - **Housekeeping:** At this stage, delete all the legacy flat-package `.go` files residing in the root `LSM-Tree` directory, as `lsmtree` will now cleanly import `github.com/shreyas/lsmtree/db`.

### Phase 8: CLI & Polish ⏳ (Pending)
- **Goal:** Create user-facing entry points and ensure absolute repository health.
- **Tasks:**
  - Add `cmd/lsm/main.go` acting as a REPL or simple CLI interface for interacting with the database.
  - Complete the `README.md` containing architectural overviews and examples.
  - Perform an extensive project-wide regression check (`go test -race -count=1 ./...`).
