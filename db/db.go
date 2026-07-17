package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shreyas/lsmtree/memtable"
	"github.com/shreyas/lsmtree/sstable"
	"github.com/shreyas/lsmtree/wal"
)

// DB — the core LSM-Tree engine

// DB is a thread-safe key-value storage engine backed by an LSM Tree.
//
// Concurrency strategy:
//   - A single sync.Mutex serialises all writes (WAL append + MemTable insert).
//     This ensures WAL ordering matches MemTable ordering.
//   - A sync.RWMutex (stateMu) guards the mutable view of MemTables and
//     SSTables. Readers acquire RLock; the flush goroutine acquires Lock
//     only for the brief moment it swaps pointers.
//   - Background flushing runs in a dedicated goroutine, triggered via an
//     unbuffered channel. It never blocks the write path for more than the
//     pointer-swap duration.
type DB struct {
	opts DBOptions

	// writeMu serialises all write operations (WAL + MemTable).
	writeMu sync.Mutex

	// stateMu protects the mutable state below. Writers hold RLock for
	// reads; the flush goroutine holds Lock for pointer swaps.
	stateMu sync.RWMutex

	wal        *wal.WAL
	active     *memtable.MemTable   // current writable MemTable
	immutables []*memtable.MemTable // MemTables pending flush (newest first)
	sstables   []*sstable.Reader    // L0 SSTables (newest first)

	// sstSeq is an atomic counter for generating unique SSTable filenames.
	sstSeq atomic.Uint64

	// flushCh signals the background goroutine to flush an immutable
	// MemTable. Capacity 1 to avoid blocking the write path.
	flushCh chan struct{}

	// compactionCh signals the background goroutine to compact SSTables.
	// Capacity 1.
	compactionCh chan struct{}

	// Lifecycle management.
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open opens or creates a DB in the given directory.
//
// On startup it:
//  1. Creates the data directory if it doesn't exist.
//  2. Opens existing SSTables (sorted by name, newest last).
//  3. Replays the WAL to reconstruct the MemTable.
//  4. Starts the background flush goroutine.
func Open(opts DBOptions) (*DB, error) {
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("db: mkdir %q: %w", opts.Dir, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	db := &DB{
		opts:         opts,
		active:       memtable.New(opts.memTableSize()),
		flushCh:      make(chan struct{}, 1),
		compactionCh: make(chan struct{}, 1),
		ctx:          ctx,
		cancel:       cancel,
	}

	// load existing SSTables
	if err := db.loadSSTables(); err != nil {
		cancel()
		return nil, fmt.Errorf("db: load sstables: %w", err)
	}

	// replay WAL
	walPath := filepath.Join(opts.Dir, "wal")
	if err := db.replayWAL(walPath); err != nil {
		db.closeSSTables()
		cancel()
		return nil, fmt.Errorf("db: replay wal: %w", err)
	}

	// open WAL for new writes
	wal, err := wal.Open(walPath)
	if err != nil {
		db.closeSSTables()
		cancel()
		return nil, fmt.Errorf("db: open wal: %w", err)
	}
	db.wal = wal

	// start background flush goroutines
	db.wg.Add(2)
	go db.flushLoop()
	go db.compactionLoop()

	return db, nil
}

// Write path

// Put inserts or updates a key-value pair.
//
// The write is first appended to the WAL for durability, then inserted
// into the active MemTable. If the MemTable exceeds its size threshold,
// it is rotated to immutable and a background flush is triggered.
func (db *DB) Put(key, value []byte) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// 1. WAL append (durability).
	if err := db.wal.Append(key, value, false); err != nil {
		return fmt.Errorf("db: put wal: %w", err)
	}
	if db.opts.SyncOnWrite {
		if err := db.wal.Sync(); err != nil {
			return fmt.Errorf("db: put sync: %w", err)
		}
	}

	// 2. MemTable insert.
	db.active.Put(key, value)

	// 3. Check threshold and rotate if needed.
	if db.active.IsFull() {
		if err := db.rotateMemTable(); err != nil {
			return fmt.Errorf("db: rotate: %w", err)
		}
	}

	return nil
}

// Delete inserts a tombstone for key, logically deleting it.
//
// The tombstone shadows any older value in immutable MemTables and
// SSTables. Physical removal happens during compaction (Phase 5).
func (db *DB) Delete(key []byte) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	if err := db.wal.Append(key, nil, true); err != nil {
		return fmt.Errorf("db: delete wal: %w", err)
	}
	if db.opts.SyncOnWrite {
		if err := db.wal.Sync(); err != nil {
			return fmt.Errorf("db: delete sync: %w", err)
		}
	}

	db.active.Delete(key)

	if db.active.IsFull() {
		if err := db.rotateMemTable(); err != nil {
			return fmt.Errorf("db: rotate: %w", err)
		}
	}

	return nil
}

// rotateMemTable freezes the active MemTable, prepends it to the
// immutables list, creates a fresh active MemTable and WAL, and
// signals the background flush goroutine.
func (db *DB) rotateMemTable() error {
	// Sync current WAL to guarantee all entries in the frozen MemTable
	// are durable before we consider them immutable.
	if err := db.wal.Sync(); err != nil {
		return fmt.Errorf("sync before rotate: %w", err)
	}

	frozen := db.active
	newActive := memtable.New(db.opts.memTableSize())

	// Close old WAL and create a new one.
	if err := db.wal.Close(); err != nil {
		return fmt.Errorf("close old wal: %w", err)
	}

	walPath := filepath.Join(db.opts.Dir, "wal")
	// Truncate the old WAL — a new one starts fresh. The frozen MemTable
	// will be flushed to an SSTable; if the process crashes before the
	// flush completes, the WAL we just closed is already durable and
	// can be replayed. For simplicity in this phase, we overwrite the
	// WAL file. A production engine would use WAL rotation with
	// sequence numbers.
	newWAL, err := wal.Open(walPath)
	if err != nil {
		return fmt.Errorf("open new wal: %w", err)
	}

	// Brief critical section: swap pointers under stateMu.
	db.stateMu.Lock()
	db.active = newActive
	db.wal = newWAL
	db.immutables = append([]*memtable.MemTable{frozen}, db.immutables...)
	db.stateMu.Unlock()

	// Signal flush (non-blocking: if a flush is already pending, the
	// goroutine will pick up all immutables in one pass).
	select {
	case db.flushCh <- struct{}{}:
	default:
	}

	return nil
}

// Read path

// Get retrieves the value associated with key.
//
// The search follows strict chronological order to ensure data freshness:
//  1. Active MemTable
//  2. Immutable MemTables (newest → oldest)
//  3. L0 SSTables (newest → oldest)
//
// If a tombstone is found at ANY level, the key is considered deleted
// and ErrKeyNotFound is returned immediately — no deeper levels are
// searched.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.stateMu.RLock()
	active := db.active
	imms := db.immutables
	ssts := db.sstables
	db.stateMu.RUnlock()

	// 1. Active MemTable.
	if val, found, deleted := active.Get(key); found {
		if deleted {
			return nil, ErrKeyNotFound
		}
		return val, nil
	}

	// 2. Immutable MemTables (newest first).
	for _, imm := range imms {
		if val, found, deleted := imm.Get(key); found {
			if deleted {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}

	// 3. L0 SSTables (newest first).
	for _, sst := range ssts {
		val, found, tombstone, err := sst.Get(key)
		if err != nil {
			return nil, fmt.Errorf("db: sstable get: %w", err)
		}
		if found {
			if tombstone {
				return nil, ErrKeyNotFound
			}
			return val, nil
		}
	}

	return nil, ErrKeyNotFound
}

// ErrKeyNotFound is returned by Get when the key does not exist or
// has been deleted (tombstoned).
var ErrKeyNotFound = fmt.Errorf("db: key not found")

// Background flush

// flushLoop is the background goroutine that flushes immutable MemTables
// to SSTables on disk.
func (db *DB) flushLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			// Drain any remaining immutables before exiting.
			db.flushAllImmutables()
			return
		case <-db.flushCh:
			db.flushAllImmutables()
		}
	}
}

// flushAllImmutables flushes every pending immutable MemTable to an
// SSTable, oldest first (so newer data always has a higher sequence
// number in the filename).
func (db *DB) flushAllImmutables() {
	for {
		db.stateMu.RLock()
		n := len(db.immutables)
		db.stateMu.RUnlock()

		if n == 0 {
			return
		}

		// Flush the oldest immutable (last element).
		db.stateMu.RLock()
		oldest := db.immutables[n-1]
		db.stateMu.RUnlock()

		if oldest.Len() == 0 {
			// Empty MemTable, just remove it.
			db.stateMu.Lock()
			db.immutables = db.immutables[:len(db.immutables)-1]
			db.stateMu.Unlock()
			continue
		}

		seq := db.sstSeq.Add(1)
		sstPath := filepath.Join(db.opts.Dir, fmt.Sprintf("sst_%06d.sst", seq))

		iter := oldest.NewIterator()
		if err := sstable.Build(sstPath, iter, oldest.Len()); err != nil {
			// Log error but don't crash — retry on next trigger.
			// In production, this would use a proper logger.
			fmt.Fprintf(os.Stderr, "db: flush error: %v\n", err)
			return
		}

		reader, err := sstable.Open(sstPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "db: open flushed sst: %v\n", err)
			return
		}

		// Atomic state update: add SSTable, remove the flushed immutable.
		db.stateMu.Lock()
		// Prepend to sstables (newest first).
		db.sstables = append([]*sstable.Reader{reader}, db.sstables...)
		// Remove the oldest immutable (last element).
		db.immutables = db.immutables[:len(db.immutables)-1]
		db.stateMu.Unlock()

		// Trigger compaction asynchronously
		select {
		case db.compactionCh <- struct{}{}:
		default:
		}
	}
}

// Startup: load SSTables and replay WAL

// loadSSTables scans the data directory for existing SSTable files and
// opens them. Files are sorted by name (lexicographic, which matches
// chronological order due to our naming scheme) and stored newest-first.
func (db *DB) loadSSTables() error {
	entries, err := os.ReadDir(db.opts.Dir)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}

	var paths []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".sst" {
			paths = append(paths, filepath.Join(db.opts.Dir, e.Name()))
		}
	}
	sort.Strings(paths) // oldest first by filename

	// Track the highest sequence number for future file naming.
	var maxSeq uint64
	for _, p := range paths {
		var seq uint64
		_, _ = fmt.Sscanf(filepath.Base(p), "sst_%06d.sst", &seq)
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	db.sstSeq.Store(maxSeq)

	// Open all SSTables (store newest-first for read path ordering).
	for i := len(paths) - 1; i >= 0; i-- {
		reader, err := sstable.Open(paths[i])
		if err != nil {
			return fmt.Errorf("open %q: %w", paths[i], err)
		}
		db.sstables = append(db.sstables, reader)
	}
	return nil
}

// replayWAL replays the Write-Ahead Log to reconstruct the MemTable.
func (db *DB) replayWAL(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil // fresh DB, nothing to replay
	}

	it, err := wal.NewIterator(path)
	if err != nil {
		return fmt.Errorf("open wal iterator: %w", err)
	}
	defer it.Close()

	count := 0
	for {
		entry, err := it.Next()
		if err != nil {
			return fmt.Errorf("wal replay: %w", err)
		}
		if entry == nil {
			break
		}
		db.active.PutWithTimestamp(entry.Key, entry.Value, entry.Timestamp, entry.Tombstone)
		count++
	}

	// If the MemTable filled up during replay, don't rotate yet —
	// let the first write after Open trigger it naturally.
	_ = count
	return nil
}

// Shutdown

// Close gracefully shuts down the database:
//  1. Cancels the context to stop the background flush goroutine.
//  2. Waits for the flush goroutine to finish (drains immutables).
//  3. Syncs and closes the WAL.
//  4. Closes all open SSTable file handles.
func (db *DB) Close() error {
	// Signal shutdown and wait for background goroutine.
	db.cancel()
	db.wg.Wait()

	// Close WAL.
	var firstErr error
	if err := db.wal.Close(); err != nil {
		firstErr = fmt.Errorf("db: close wal: %w", err)
	}

	// Close all SSTables.
	db.closeSSTables()

	return firstErr
}

func (db *DB) closeSSTables() {
	for _, sst := range db.sstables {
		sst.Close()
	}
}

// Utilities

func (db *DB) SSTCount() int {
	db.stateMu.RLock()
	defer db.stateMu.RUnlock()
	return len(db.sstables)
}

func (db *DB) ImmutableCount() int {
	db.stateMu.RLock()
	defer db.stateMu.RUnlock()
	return len(db.immutables)
}

// waitForFlush blocks until all immutable MemTables have been flushed.
func (db *DB) waitForFlush() {
	for {
		db.stateMu.RLock()
		n := len(db.immutables)
		db.stateMu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
