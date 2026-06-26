package db

// DBOptions controls engine behaviour.
type DBOptions struct {
	// Dir is the directory for all data files (WAL, SSTables).
	Dir string

	// MemTableSize is the approximate byte threshold at which the active
	// MemTable is rotated to immutable and scheduled for flushing.
	// Default: 4 MB.
	MemTableSize int64

	// SyncOnWrite controls whether every Put/Delete call triggers an
	// fsync on the WAL. When true, each mutation is durable before the
	// call returns (highest safety). When false, the WAL is fsynced
	// periodically or on Close (higher throughput, small data-loss window).
	SyncOnWrite bool

	// CompactionThreshold is the number of L0 SSTables that triggers a compaction.
	// Default: 4.
	CompactionThreshold int
}

func (o *DBOptions) memTableSize() int64 {
	if o.MemTableSize > 0 {
		return o.MemTableSize
	}
	return 4 * 1024 * 1024 // 4 MB
}
