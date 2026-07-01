package db

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/shreyas/lsmtree/compaction"
	"github.com/shreyas/lsmtree/iterator"
	"github.com/shreyas/lsmtree/sstable"
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
	tablesToCompact := make([]*sstable.Reader, len(db.sstables))
	copy(tablesToCompact, db.sstables) // newest first
	db.stateMu.RUnlock()

	iters := make([]iterator.Iterator, 0, len(tablesToCompact))
	for _, sst := range tablesToCompact {
		iters = append(iters, sst.NewIterator())
	}

	mergeIt := compaction.NewMergeIterator(iters)
	defer mergeIt.Close()

	seq := db.sstSeq.Add(1)
	newSSTPath := filepath.Join(db.opts.Dir, fmt.Sprintf("sst_%06d.sst", seq))

	// For tombstone purging during a full compaction, we can drop tombstones
	// because there are no older files that could contain the shadowed value.
	// We will wrap the merge iterator to drop tombstones.
	purgeIt := compaction.NewPurgeIterator(mergeIt)

	if err := sstable.Build(newSSTPath, purgeIt); err != nil {
		fmt.Fprintf(os.Stderr, "db: compaction build error: %v\n", err)
		return
	}

	// 4. Open the new SSTable.
	newReader, err := sstable.Open(newSSTPath)
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

	newSSTables := make([]*sstable.Reader, 0, newFlushes+1)
	newSSTables = append(newSSTables, db.sstables[:newFlushes]...) // keep newly flushed tables
	newSSTables = append(newSSTables, newReader)                   // add the single compacted table

	db.sstables = newSSTables
	db.stateMu.Unlock()

	// 6. Close and delete the old SSTables.
	for _, sst := range tablesToCompact {
		sst.Close()
		os.Remove(sst.Path())
	}
}
