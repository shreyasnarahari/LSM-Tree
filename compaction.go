package lsmtree

import (
	"fmt"
	"os"
	"path/filepath"
)

// compactionLoop is the background goroutine that merges multiple SSTables
// into a single SSTable to reclaim space (purging tombstones) and improve
// read performance.
func (db *DB) compactionLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-db.compactionCh:
			db.runCompaction()
		}
	}
}

// runCompaction checks if the compaction threshold is met, and if so,
// compacts all existing SSTables into a single new one.
func (db *DB) runCompaction() {
	db.stateMu.RLock()
	sstCount := len(db.sstables)
	db.stateMu.RUnlock()

	threshold := db.opts.CompactionThreshold
	if threshold <= 0 {
		threshold = 4 // Default to 4
	}

	if sstCount < threshold {
		return
	}

	// For this implementation, we do a "full" compaction of all L0 tables.
	// Grab the current snapshot of SSTables to compact.
	db.stateMu.RLock()
	tablesToCompact := make([]*SSTableReader, len(db.sstables))
	copy(tablesToCompact, db.sstables) // newest first
	db.stateMu.RUnlock()

	// 1. Create iterators for all tables.
	iters := make([]Iterator, 0, len(tablesToCompact))
	for _, sst := range tablesToCompact {
		iters = append(iters, sst.Iterator())
	}

	// 2. Create the MergeIterator.
	mergeIt := NewMergeIterator(iters)
	defer mergeIt.Close()

	// 3. Create a MemTable wrapper around the MergeIterator to reuse BuildSSTable.
	// We need to write the merged entries to a new SSTable.
	// BuildSSTable expects a MemTableIterator, but we have a generic Iterator.
	// Wait, BuildSSTable takes *MemTableIterator specifically.
	// We should change BuildSSTable to accept the Iterator interface instead!
	
	// Let's create the new SSTable.
	seq := db.sstSeq.Add(1)
	newSSTPath := filepath.Join(db.opts.Dir, fmt.Sprintf("sst_%06d.sst", seq))
	
	// For tombstone purging during a full compaction, we can drop tombstones
	// because there are no older files that could contain the shadowed value.
	// We will wrap the merge iterator to drop tombstones.
	purgeIt := &purgeIterator{
		it: mergeIt,
	}
	purgeIt.advanceToValid()

	// We don't know the exact count for the bloom filter, but we can estimate
	// based on the old tables. Let's sum the block counts (rough proxy).
	// For simplicity, we just use 100,000 as expectedItems if we don't know,
	// but BuildSSTable takes expectedItems. Let's estimate it.
	expectedItems := 0
	for _, sst := range tablesToCompact {
		// ~100 items per 4KB block is a reasonable guess
		expectedItems += sst.BlockCount() * 100
	}

	if err := BuildSSTable(newSSTPath, purgeIt, expectedItems); err != nil {
		fmt.Fprintf(os.Stderr, "db: compaction build error: %v\n", err)
		return
	}

	// 4. Open the new SSTable.
	newReader, err := OpenSSTable(newSSTPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db: open compacted sst: %v\n", err)
		return
	}

	// 5. Atomically swap the new SSTable into the DB state.
	db.stateMu.Lock()
	// Since new flushes might have occurred during compaction, we only replace
	// the tables we actually compacted. The newest tables are at the front.
	// db.sstables = [flushed_new_1, flushed_new_2, compacted_1, compacted_2, ...]
	// We replace the suffix of db.sstables that matches tablesToCompact.
	
	// Identify how many new tables were added during compaction.
	newFlushes := len(db.sstables) - len(tablesToCompact)
	
	newSSTables := make([]*SSTableReader, 0, newFlushes+1)
	newSSTables = append(newSSTables, db.sstables[:newFlushes]...) // keep newly flushed tables
	newSSTables = append(newSSTables, newReader) // add the single compacted table
	
	db.sstables = newSSTables
	db.stateMu.Unlock()

	// 6. Close and delete the old SSTables.
	for _, sst := range tablesToCompact {
		sst.Close()
		os.Remove(sst.Path())
	}
}

// purgeIterator wraps an Iterator and drops all tombstones.
// It is only safe to use during a FULL compaction.
type purgeIterator struct {
	it Iterator
}

func (p *purgeIterator) advanceToValid() {
	for p.it.Valid() && p.it.Tombstone() {
		p.it.Next()
	}
}

func (p *purgeIterator) Valid() bool { return p.it.Valid() }
func (p *purgeIterator) Next() {
	p.it.Next()
	p.advanceToValid()
}
func (p *purgeIterator) Key() []byte { return p.it.Key() }
func (p *purgeIterator) Value() []byte { return p.it.Value() }
func (p *purgeIterator) Timestamp() uint64 { return p.it.Timestamp() }
func (p *purgeIterator) Tombstone() bool { return p.it.Tombstone() }
func (p *purgeIterator) Error() error { return p.it.Error() }
func (p *purgeIterator) Close() error { return p.it.Close() }
